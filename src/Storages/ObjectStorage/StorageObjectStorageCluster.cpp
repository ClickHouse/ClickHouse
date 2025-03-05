#include "Storages/ObjectStorage/StorageObjectStorageCluster.h"

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/extractTableFunctionArgumentsFromSelectQuery.h>
#include <Common/SipHash.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool use_hive_partitioning;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

UInt32 FileToNodeCache::getNodeForFile(const String & file_path, UInt32 total_nodes)
{
    if (total_nodes == 0)
        return 0;
        
    std::lock_guard lock(mutex);
    auto it = file_to_node_map.find(file_path);
    
    if (it != file_to_node_map.end())
        return it->second;
    
    // Use a hash of the file path to determine the node
    // This ensures consistent assignment even if the cache is cleared
    UInt64 hash_value = sipHash64(file_path.data(), file_path.size());
    UInt32 node_id = hash_value % total_nodes;
    
    // Log the hash calculation for debugging
    LOG_TRACE(
        getLogger("FileToNodeCache"),
        "Assigning file {} to node {} (hash: {}, total_nodes: {})",
        file_path,
        node_id,
        hash_value,
        total_nodes
    );
    
    file_to_node_map[file_path] = node_id;
    return node_id;
}

void FileToNodeCache::clear()
{
    std::lock_guard lock(mutex);
    file_to_node_map.clear();
}

String StorageObjectStorageCluster::getPathSample(StorageInMemoryMetadata metadata, ContextPtr context)
{
    auto query_settings = configuration->getQuerySettings(context);
    /// We don't want to throw an exception if there are no files with specified path.
    query_settings.throw_on_zero_files_match = false;
    auto file_iterator = StorageObjectStorageSource::createFileIterator(
        configuration,
        query_settings,
        object_storage,
        false, // distributed_processing
        context,
        {}, // predicate
        metadata.getColumns().getAll(), // virtual_columns
        nullptr, // read_keys
        {} // file_progress_callback
    );

    if (auto file = file_iterator->next(0))
        return file->getPath();
    return "";
}

StorageObjectStorageCluster::StorageObjectStorageCluster(
    const String & cluster_name_,
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorageCluster(
        cluster_name_, table_id_, getLogger(fmt::format("{}({})", configuration_->getEngineName(), table_id_.table_name)))
    , configuration{configuration_}
    , object_storage(object_storage_)
{
    ColumnsDescription columns{columns_};
    std::string sample_path;
    resolveSchemaAndFormat(columns, configuration->format, object_storage, configuration, {}, sample_path, context_);
    configuration->check(context_);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    metadata.setConstraints(constraints_);

    if (sample_path.empty() && context_->getSettingsRef()[Setting::use_hive_partitioning])
        sample_path = getPathSample(metadata, context_);

    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(metadata.columns, context_, sample_path));
    setInMemoryMetadata(metadata);
}

std::string StorageObjectStorageCluster::getName() const
{
    return configuration->getEngineName();
}

void StorageObjectStorageCluster::updateQueryToSendIfNeeded(
    ASTPtr & query,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    ASTExpressionList * expression_list = extractTableFunctionArgumentsFromSelectQuery(query);
    if (!expression_list)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected SELECT query from table function {}, got '{}'",
            configuration->getEngineName(), queryToString(query));
    }

    ASTs & args = expression_list->children;
    const auto & structure = storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription();
    if (args.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected empty list of arguments for {}Cluster table function",
            configuration->getEngineName());
    }

    ASTPtr cluster_name_arg = args.front();
    args.erase(args.begin());
    configuration->addStructureAndFormatToArgsIfNeeded(args, structure, configuration->format, context, /*with_structure=*/true);
    args.insert(args.begin(), cluster_name_arg);
}

RemoteQueryExecutor::Extension StorageObjectStorageCluster::getTaskIteratorExtension(
    const ActionsDAG::Node * predicate, const ContextPtr & local_context) const
{
    auto iterator = StorageObjectStorageSource::createFileIterator(
        configuration, configuration->getQuerySettings(local_context), object_storage, /* distributed_processing */false,
        local_context, predicate, virtual_columns, nullptr, local_context->getFileProgressCallback());

    // Get the cluster to determine the number of nodes
    auto cluster = local_context->getCluster(cluster_name);
    
    // Create a shared state for the callback
    struct SharedState
    {
        std::mutex mutex;
        std::unordered_map<String, std::vector<String>> connection_to_files;
        bool initialized = false;
    };
    
    auto shared_state = std::make_shared<SharedState>();
    
    auto callback = std::make_shared<TaskIterator>([iterator, shared_state, this, cluster]
        (Connection * connection) mutable -> String
    {
        // Initialize the mapping of files to nodes if not done yet
        if (!shared_state->initialized)
        {
            std::lock_guard lock(shared_state->mutex);
            if (!shared_state->initialized)
            {
                // Pre-scan all files and assign them to nodes
                std::vector<String> all_files;
                while (auto object_info = iterator->next(0))
                {
                    all_files.push_back(object_info->getPath());
                }
                
                // Create a mapping from shard index to all possible connection keys
                std::unordered_map<UInt32, String> shard_to_connection_keys;
                const auto & shards_info = cluster->getShardsInfo();
                
                size_t node_idx = 0;
                for (size_t shard_idx = 0; shard_idx < shards_info.size(); ++shard_idx)
                {
                    const auto & shard = shards_info[shard_idx];

                    // For each shard, create a connection key for each replica
                    // for (const auto & address : shard.local_addresses)
                    // {
                    //     String connection_key = address.host_name + ":" + std::to_string(address.port);
                    //     shard_to_connection_keys[node_idx++] = connection_key;

                    //     LOG_TRACE(
                    //         getLogger("StorageObjectStorageCluster"),
                    //         "Discovered shard {} with connection key {}",
                    //         shard_idx,
                    //         connection_key
                    //     );
                    // }
                    
                    for (size_t replica_idx = 0; replica_idx < shard.per_replica_pools.size(); ++replica_idx)
                    {
                        if (!shard.per_replica_pools[replica_idx])
                            continue;
                        
                        const auto & pool = shard.per_replica_pools[replica_idx];
                        String connection_key = pool->getHost() + ":" + std::to_string(pool->getPort());
                        shard_to_connection_keys[node_idx++] = connection_key;
                        
                        LOG_TRACE(
                            getLogger("StorageObjectStorageCluster"),
                            "Discovered shard {} with connection key {} (remote)",
                            shard_idx,
                            connection_key
                        );
                    }
                }
                
                UInt32 total_nodes = shard_to_connection_keys.size();
                
                // Assign files to nodes based on the hash
                for (const auto & file_path : all_files)
                {
                    UInt32 shard_idx = file_node_cache.getNodeForFile(file_path, total_nodes);
                    
                    // Add this file to all possible connection keys for this shard
                    if (shard_to_connection_keys.contains(shard_idx))
                    {
                        shared_state->connection_to_files[shard_to_connection_keys[shard_idx]].push_back(file_path);
                            
                        LOG_TRACE(
                            getLogger("StorageObjectStorageCluster"),
                            "Assigned file {} to shard {} with connection key {}",
                            file_path,
                            shard_idx,
                            shard_to_connection_keys[shard_idx]
                        );
                    }
                }
                
                shared_state->initialized = true;
            }
        }
        
        // Get the connection key
        String connection_key;
        if (connection)
        {
            connection_key = connection->getHost() + ":" + std::to_string(connection->getPort());
        }
        else
        {
            // Default key for null connection
            connection_key = "default";
        }
        
        // Get the next file for this connection
        std::lock_guard lock(shared_state->mutex);
        auto & files_for_connection = shared_state->connection_to_files[connection_key];
        
        if (files_for_connection.empty())
        {
            LOG_TRACE(
                getLogger("StorageObjectStorageCluster"),
                "No more files for connection {}",
                connection_key
            );
            return "";
        }
            
        String next_file = files_for_connection.back();
        files_for_connection.pop_back();
        
        LOG_TRACE(
            getLogger("StorageObjectStorageCluster"),
            "Assigning file {} to connection {}",
            next_file,
            connection_key
        );
        
        return next_file;
    });
    
    return RemoteQueryExecutor::Extension{ .task_iterator = std::move(callback) };
}

}
