#include <Common/config.h>

#if USE_HDFS
#include <Storages/StorageHDFS.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>

namespace DB
{
StoragePtr TableFunctionHDFS::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name) const
{
    return StorageHDFS::create(source,
        getDatabaseName(),
        table_name,
        format,
        columns,
        global_context);
}

void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}
}
#endif
