#include "config.h"

#if USE_AWS_S3

#include <Storages/DataLakes/StorageHudi.h>
#include <TableFunctions/ITableFunctionDataLake.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include "registerTableFunctions.h"

namespace DB
{

struct TableFunctionHudiName
{
    static constexpr auto name = "hudi";
};
using TableFunctionHudi = ITableFunctionDataLake<TableFunctionHudiName, StorageHudiS3, TableFunctionS3>;

void registerTableFunctionHudi(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHudi>(
        {.documentation
         = {.description=R"(The table function can be used to read the Hudi table stored on object store.)",
            .examples{{"hudi", "SELECT * FROM hudi(url, access_key_id, secret_access_key)", ""}},
            .categories{"DataLake"}},
         .allow_readonly = false});
}
}

#endif
