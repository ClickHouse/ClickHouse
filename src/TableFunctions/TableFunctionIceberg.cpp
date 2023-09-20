#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Storages/DataLakes/StorageIceberg.h>
#include <TableFunctions/ITableFunctionDataLake.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include "registerTableFunctions.h"


namespace DB
{

struct TableFunctionIcebergName
{
    static constexpr auto name = "iceberg";
};

using TableFunctionIceberg = ITableFunctionDataLake<TableFunctionIcebergName, StorageIcebergS3, TableFunctionS3>;

void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionIceberg>(
        {.documentation
         = {.description=R"(The table function can be used to read the Iceberg table stored on object store.)",
            .examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}

}

#endif
