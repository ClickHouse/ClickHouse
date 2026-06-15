#include <TableFunctions/TableFunctionURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

StoragePtr TableFunctionURLCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/, bool /*is_insert_query*/) const
{
    const auto & client_info = context->getClientInfo();
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// Only enable task-based reading when the initiator installed a cluster-function read-task
        /// iterator for us. When this urlCluster is nested under a plain Distributed /
        /// parallel-replicas broadcast the initiator has no such iterator, so a ReadTaskRequest
        /// would hit a LOGICAL_ERROR ("Distributed task iterator is not initialized", issue #91736).
        const bool distributed_processing = context->canUseClusterFunctionDistributedRead();

        //On worker node this uri won't contain globs
        return std::make_shared<StorageURL>(
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
            distributed_processing);
    }

    return std::make_shared<StorageURLCluster>(
        context,
        cluster_name,
        filename,
        format,
        compression_method,
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context, true),
        ConstraintsDescription{},
        configuration);
}

void registerTableFunctionURLCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURLCluster>({});
}

}
