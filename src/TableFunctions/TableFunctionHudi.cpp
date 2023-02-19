#include "config.h"

#if USE_AWS_S3

#    include <Storages/StorageHudi.h>
#    include <TableFunctions/ITableFunctionDataLake.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionS3.h>
#    include "registerTableFunctions.h"

namespace DB
{

struct TableFunctionHudiName
{
    static constexpr auto name = "hudi";
};
using TableFunctionHudi = ITableFunctionDataLake<TableFunctionHudiName, StorageHudi, TableFunctionS3>;

void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {R"(The table function can be used to read the Hudi table stored on object store.)",
            Documentation::Examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)"}},
            Documentation::Categories{"DataLake"}},
         .allow_readonly = false});
}
}

#endif
