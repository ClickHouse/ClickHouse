#include <Core/Settings.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFileCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <TableFunctions/registerTableFunctions.h>

#include <memory>

namespace DB
{
namespace Setting
{
    extern const SettingsString rename_files_after_processing;
}

StoragePtr TableFunctionFileCluster::getStorage(
    const String & /*source*/, const String & /*format_*/, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & /*compression_method_*/, bool /*is_insert_query*/) const
{
    StoragePtr storage;

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// Only enable task-based reading when the initiator installed a cluster-function read-task
        /// iterator for us. When this fileCluster is nested under a plain Distributed /
        /// parallel-replicas broadcast the initiator has no such iterator, so a ReadTaskRequest
        /// would hit a LOGICAL_ERROR ("Distributed task iterator is not initialized", issue #91736).
        const bool distributed_processing = context->canUseClusterFunctionDistributedRead();

        /// On worker node this filename won't contain any globs
        StorageFile::CommonArguments args{
            WithContext(context),
            StorageID(getDatabaseName(), table_name),
            format,
            std::nullopt /*format settings*/,
            compression_method,
            columns,
            ConstraintsDescription{},
            String{},
            context->getSettingsRef()[Setting::rename_files_after_processing]};

        storage = std::make_shared<StorageFile>(StorageFile::FileSource::parse(filename, context), distributed_processing, args);
    }
    else
    {
        storage = std::make_shared<StorageFileCluster>(
            context,
            cluster_name,
            filename,
            format,
            compression_method,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{});
    }

    return storage;
}


void registerTableFunctionFileCluster(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFileCluster>(
        {
            .description = R"(This table function is used for distributed reading of files in cluster nodes filesystems.)",
            .examples{{"fileCluster", "SELECT * from fileCluster('my_cluster', 'file{1,2}.csv');", ""}},
            .category = FunctionDocumentation::Category::TableFunction
        },
        {.allow_readonly = false});
}

}
