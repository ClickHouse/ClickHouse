#include <Storages/StorageURL.h>
#include <Storages/ColumnsDescription.h>
#include <Access/AccessFlags.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Poco/URI.h>
#include "registerTableFunctions.h"


namespace DB
{
StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context,
    const std::string & table_name, const String & compression_method, GetStructureFunc get_structure) const
{
    Poco::URI uri(source);
    return std::make_shared<StorageTableFunction<StorageURL>>(std::move(get_structure), uri,
            StorageID(getDatabaseName(), table_name), format, columns, ConstraintsDescription{},
            global_context, compression_method);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
