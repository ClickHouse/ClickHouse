#include <Storages/StorageURL.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Poco/URI.h>


namespace DB
{
StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name) const
{
    Poco::URI uri(source);
    return StorageURL::create(uri, getDatabaseName(), table_name, format, columns, ConstraintsDescription{}, global_context);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
