#include "config.h"

#if USE_AWS_S3 && USE_PARQUET

#include <Storages/DataLakes/StorageDeltaLake.h>
#include <TableFunctions/ITableFunctionDataLake.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include "registerTableFunctions.h"

namespace DB
{

struct TableFunctionDeltaLakeName
{
    static constexpr auto name = "deltaLake";
};

using TableFunctionDeltaLake = ITableFunctionDataLake<TableFunctionDeltaLakeName, StorageDeltaLakeS3, TableFunctionS3>;

void registerTableFunctionDeltaLake(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaLake>({.allow_readonly = false});
}

}

#endif
