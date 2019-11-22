#include <Storages/StorageFile.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFile.h>

namespace DB
{
StoragePtr TableFunctionFile::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name) const
{
    return StorageFile::create(source,
        -1,
        global_context.getUserFilesPath(),
        getDatabaseName(),
        table_name,
        format,
        columns,
        ConstraintsDescription{},
        global_context);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}
}
