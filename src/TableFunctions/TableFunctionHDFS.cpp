#include <Common/config.h>
#include "registerTableFunctions.h"

#if USE_HDFS
#include <Storages/StorageHDFS.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>

namespace DB
{
StoragePtr TableFunctionHDFS::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, Context & global_context,
    const std::string & table_name, const String & compression_method_) const
{
    return StorageHDFS::create(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        columns,
        ConstraintsDescription{},
        global_context,
        compression_method_);
}


#if USE_HDFS
void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}
#endif
}
#endif
