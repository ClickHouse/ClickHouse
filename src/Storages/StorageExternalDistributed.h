#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

/// Storages MySQL and PostgreSQL use ConnectionPoolWithFailover and support multiple replicas.
/// This class unites multiple storages with replicas into multiple shards with replicas.
/// A query to external database is passed to one replica on each shard, the result is united.
/// Replicas on each shard have the same priority, traversed replicas are moved to the end of the queue.
/// TODO: try `load_balancing` setting for replicas priorities same way as for table function `remote`
class StorageExternalDistributed final : public shared_ptr_helper<StorageExternalDistributed>, public DB::IStorage
{
    friend struct shared_ptr_helper<StorageExternalDistributed>;

public:
    enum class ExternalStorageEngine
    {
        MySQL,
        PostgreSQL,
        URL
    };

    std::string getName() const override { return "ExternalDistributed"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageExternalDistributed(
        const StorageID & table_id_,
        ExternalStorageEngine table_engine,
        const String & cluster_description,
        const String & remote_database_,
        const String & remote_table_,
        const String & username,
        const String & password,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_);

    StorageExternalDistributed(
        const String & addresses_description,
        const StorageID & table_id,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const String & compression_method,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        ContextPtr context);

private:
    using Shards = std::unordered_set<StoragePtr>;
    Shards shards;
};

}
