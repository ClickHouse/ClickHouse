#include "config.h"

#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionIcebergCluster.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/DataLakes/StorageIcebergCluster.h>
#include <Storages/DataLakes/StorageIceberg.h>
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>


namespace DB
{

template <typename Definition, typename Configuration>
StoragePtr TableFunctionIcebergClusterImpl<Definition, Configuration>::executeImpl(
    const ASTPtr & /*function*/, ContextPtr context,
    const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    auto configuration = Base::getConfiguration(context);

    ColumnsDescription columns;
    if (configuration->structure != "auto")
        columns = parseColumnsListFromString(configuration->structure, context);
    else if (!Base::structure_hint.empty())
        columns = Base::structure_hint;
    else if (!cached_columns.empty())
        columns = cached_columns;

    auto object_storage = Base::getObjectStorage(context, !is_insert_query);
    StoragePtr storage;

    const auto & client_info = context->getClientInfo();

    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        bool can_use_distributed_iterator =
            client_info.collaborate_with_initiator &&
            context->hasClusterFunctionReadTaskCallback();

        /// On worker node this filename won't contains globs
        storage = std::make_shared<StorageIceberg>(
            configuration,
            object_storage,
            context,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            /* comment */ String{},
            /* format_settings */ std::nullopt, /// No format_settings
            /* mode */ LoadingStrictnessLevel::CREATE,
            /* catalog*/nullptr,
            /* if_not_exists*/false,
            /* is_datalake_query*/ false,
            /* distributed_processing */ can_use_distributed_iterator,
            /* partition_by_ */Base::partition_by,
            /* order_by_ */nullptr,
            /* is_table_function */true,
            /* lazy_init */ true);
    }
    else
    {
        storage = std::make_shared<StorageIcebergCluster>(
            ITableFunctionCluster<Base>::cluster_name,
            configuration,
            object_storage,
            StorageID(Base::getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            Base::partition_by,
            context,
            /* is_table_function */true);
    }

    storage->startup();
    return storage;
}

#if USE_AVRO
template class TableFunctionIcebergClusterImpl<IcebergLocalClusterDefinition, StorageLocalIcebergConfiguration>;
#endif
#if USE_AVRO && USE_AWS_S3
template class TableFunctionIcebergClusterImpl<IcebergS3ClusterDefinition, StorageS3IcebergConfiguration>;
#endif
#if USE_AVRO && USE_AWS_S3
template class TableFunctionIcebergClusterImpl<IcebergClusterDefinition, StorageS3IcebergConfiguration>;
#endif
#if USE_AVRO && USE_AZURE_BLOB_STORAGE
template class TableFunctionIcebergClusterImpl<IcebergAzureClusterDefinition, StorageAzureIcebergConfiguration>;
#endif
#if USE_AVRO && USE_HDFS
template class TableFunctionIcebergClusterImpl<IcebergHDFSClusterDefinition, StorageHDFSIcebergConfiguration>;
#endif

}
