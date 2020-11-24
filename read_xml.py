
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.functions import *

xml_sourcce_path = "sample.xml"

df = spark.read \
.format('xml') \
.option("rootTag", "EnvoCad") \
.option("rowTag", "Cli") \
.option("valueTag", "_VALUE") \
.load(xml_sourcce_path)
# .option("attributePrefix", "__") \

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

flatten_cols = flatten(df.schema)

flatten_df = df.select(*flatten_cols)

df.printSchema()

flatten_df.printSchema()

df.write.mode('overwrite').parquet('output.pq')
