#include "config.h"

#if USE_AWS_S3

#    include <Storages/StorageIceberg.h>
#    include <TableFunctions/ITableFunctionDataLake.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include "registerTableFunctions.h"


namespace DB
{

struct TableFunctionIcebergName
{
    static constexpr auto name = "iceberg";
};

using TableFunctionIceberg = ITableFunctionDataLake<TableFunctionIcebergName, StorageIceberg>;

void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionIceberg>(
        {.documentation
         = {R"(The table function can be used to read the Iceberg table stored on object store.)",
            Documentation::Examples{{"iceberg", "SELECT * FROM iceberg(url, access_key_id, secret_access_key)"}},
            Documentation::Categories{"DataLake"}},
         .allow_readonly = false});
}

}

#endif
