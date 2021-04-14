#include <Storages/StorageExecutable.h>
#include <Storages/ColumnsDescription.h>
#include <Access/AccessFlags.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionExecutable.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include "registerTableFunctions.h"

namespace DB
{
StoragePtr TableFunctionExecutable::getStorage(
    const String & source_path, const String & format_, const ColumnsDescription & columns, Context & global_context, const std::string & table_name, const std::string & compression_method_) const
{
    return StorageExecutable::create(StorageID(getDatabaseName(), table_name), format_, source_path, columns, ConstraintsDescription{}, global_context, chooseCompressionMethod("", compression_method_));
}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}
}
