#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/ITableFunctionDataLake.h>

namespace DB
{

#if USE_AWS_S3
#if USE_AVRO
void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionIceberg>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the Iceberg table stored on object store.)",
            .examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}
        },
        .allow_readonly = false
    });
}
#endif

#if USE_PARQUET
void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaLake>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the DeltaLake table stored on object store.)",
            .examples{{"deltaLake", "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}
        },
        .allow_readonly = false
    });
}
#endif

void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
    {
        .documentation =
        {
            .description=R"(The table function can be used to read the Hudi table stored on object store.)",
            .examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}
        },
        .allow_readonly = false
    });
}
#endif

void registerDataLakeTableFunctions(TableFunctionFactory & factory)
{
    UNUSED(factory);
#if USE_AWS_S3
#if USE_AVRO
    registerTableFunctionIceberg(factory);
#endif
#if USE_PARQUET
    registerTableFunctionDeltaLake(factory);
#endif
    registerTableFunctionHudi(factory);
#endif
}

}
