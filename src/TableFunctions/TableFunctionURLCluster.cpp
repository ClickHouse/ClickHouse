#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include "registerTableFunctions.h"

namespace DB
{

StoragePtr TableFunctionURLCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/) const
{
    StoragePtr storage;
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        //On worker node this uri won't contain globs
        storage = std::make_shared<StorageURL>(
            filename,
            StorageID(getDatabaseName(), table_name),
            format,
            std::nullopt /*format settings*/,
            columns,
            ConstraintsDescription{},
            String{},
            context,
            compression_method,
            configuration.headers,
            configuration.http_method,
            nullptr,
            /*distributed_processing=*/ true);
    }
    else
    {
        storage = std::make_shared<StorageURLCluster>(
            context,
            cluster_name,
            filename,
            format,
            compression_method,
            StorageID(getDatabaseName(), table_name),
            getActualTableStructure(context, /* is_insert_query */ true),
            ConstraintsDescription{},
            configuration,
            structure != "auto");
    }
    return storage;
}

void registerTableFunctionURLCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURLCluster>();
}

}
