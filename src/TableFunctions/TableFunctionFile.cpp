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
    const String & source, const String & format_, const ColumnsDescription & columns, Context & global_context,
    const std::string & table_name, const std::string & compression_method_) const
{
    StorageFile::CommonArguments args{StorageID(getDatabaseName(), table_name), format_, compression_method_, columns, ConstraintsDescription{}, global_context};

    return StorageFile::create(source, global_context.getUserFilesPath(), args);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}
}
