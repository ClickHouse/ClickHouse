#include "config.h"
#include "registerTableFunctions.h"

#if USE_HDFS
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>

namespace DB
{

StoragePtr TableFunctionHDFS::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
    return std::make_shared<StorageHDFS>(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        columns,
        ConstraintsDescription{},
        String{},
        global_context,
        compression_method_);
}

ColumnsDescription TableFunctionHDFS::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        if (format == "auto")
            return StorageHDFS::getTableStructureAndFormatFromData(filename, compression_method, context).first;
        return StorageHDFS::getTableStructureFromData(format, filename, compression_method, context);
    }

    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}

}
#endif
