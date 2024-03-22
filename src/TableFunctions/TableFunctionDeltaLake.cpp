#include "config.h"

#if USE_AWS_S3

#    include <Storages/StorageDeltaLake.h>
#    include <TableFunctions/ITableFunctionDataLake.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionS3.h>
#    include "registerTableFunctions.h"

namespace DB
{

struct TableFunctionDeltaLakeName
{
    static constexpr auto name = "deltaLake";
};

using TableFunctionDeltaLake = ITableFunctionDataLake<TableFunctionDeltaLakeName, StorageDeltaLake, TableFunctionS3>;

void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaLake>(
        {.documentation
         = {R"(The table function can be used to read the DeltaLake table stored on object store.)",
            Documentation::Examples{{"deltaLake", "SELECT * FROM deltaLake(url, access_key_id, secret_access_key)"}},
            Documentation::Categories{"DataLake"}},
         .allow_readonly = false});
}

}

#endif
