#include <Common/config.h>

#if USE_HDFS
#include <Storages/StorageHDFS.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>

namespace DB
{
StoragePtr TableFunctionHDFS::getStorage(
    const String & source, const String & format, const Block & sample_block, Context & global_context) const
{
    return StorageHDFS::create(source,
        getName(),
        format,
        ColumnsDescription{sample_block.getNamesAndTypesList()},
        global_context);
}

void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}
}
#endif
