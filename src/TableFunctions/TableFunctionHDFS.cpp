#include <Common/config.h>
#include "registerTableFunctions.h"

#if USE_HDFS
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

namespace DB
{

StoragePtr TableFunctionHDFS::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
    return StorageHDFS::create(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        columns,
        ConstraintsDescription{},
        String{},
        global_context,
        compression_method_);
}

ColumnsDescription TableFunctionHDFS::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
        return StorageHDFS::getTableStructureFromData(format, filename, compression_method, context);

    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}

}
#endif
