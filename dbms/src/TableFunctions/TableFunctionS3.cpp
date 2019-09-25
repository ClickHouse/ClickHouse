#include <Storages/StorageS3.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <Poco/URI.h>

namespace DB
{

StoragePtr TableFunctionS3::getStorage(
    const String & source, const String & format, const ColumnsDescription & columns, Context & global_context, const std::string & table_name) const
{
    Poco::URI uri(source);
    UInt64 min_upload_part_size = global_context.getSettingsRef().s3_min_upload_part_size;
    return StorageS3::create(uri, getDatabaseName(), table_name, format, min_upload_part_size, columns, ConstraintsDescription{}, global_context);
}

void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

}
