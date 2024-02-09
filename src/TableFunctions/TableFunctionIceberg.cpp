#include "config.h"

#if USE_AWS_S3 && USE_AVRO

#include <Storages/DataLakes/Iceberg/StorageIceberg.h>
#include <TableFunctions/ITableFunctionDataLake.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionObjectStorage.h>
#include "registerTableFunctions.h"


namespace DB
{

struct TableFunctionIcebergName
{
    static constexpr auto name = "iceberg";
};

using TableFunctionIceberg = ITableFunctionDataLake<
    TableFunctionIcebergName,
    StorageIceberg<S3StorageSettings>,
    TableFunctionS3>;

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
