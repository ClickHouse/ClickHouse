#include <Storages/StorageFile.h>
#include <Storages/ColumnsDescription.h>
#include <Access/AccessFlags.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFile.h>
#include <Interpreters/Context.h>
#include "registerTableFunctions.h"

namespace DB
{
StoragePtr TableFunctionFile::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context,
    const std::string & table_name, const std::string & compression_method, GetStructureFunc get_structure) const
{
    StorageFile::CommonArguments args{StorageID(getDatabaseName(), table_name), format, compression_method, columns, ConstraintsDescription{}, global_context};

    return std::make_shared<StorageTableFunction<StorageFile>>(std::move(get_structure), source, global_context.getUserFilesPath(), args);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}
}
