#include <Storages/StorageURL.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionURL.h>
#include <Poco/URI.h>

namespace DB
{
StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format, const Block & sample_block, Context & global_context) const
{
    Poco::URI uri(source);
    return StorageURL::create(uri, getName(), format, ColumnsDescription{sample_block.getNamesAndTypesList()}, global_context);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
