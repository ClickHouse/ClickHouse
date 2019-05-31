#include <Storages/StorageS3.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <Poco/URI.h>

namespace DB
{
StoragePtr TableFunctionS3::getStorage(
    const String & source, const String & format, const Block & sample_block, Context & global_context) const
{
    Poco::URI uri(source);
    return StorageS3::create(uri, getName(), format, ColumnsDescription{sample_block.getNamesAndTypesList()}, global_context);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}
}
