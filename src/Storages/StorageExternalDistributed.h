#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_MYSQL

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <mysqlxx/PoolWithFailover.h>


namespace DB
{

/// Storages MySQL and PostgreSQL use ConnectionPoolWithFailover and support multiple replicas.
/// This is a class which unites multiple storages with replicas into multiple shards with replicas.
class StorageExternalDistributed final : public ext::shared_ptr_helper<StorageExternalDistributed>, public DB::IStorage
{
    friend struct ext::shared_ptr_helper<StorageExternalDistributed>;

public:
    std::string getName() const override { return "ExternalDistributed"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageExternalDistributed(
        const StorageID & table_id_,
        const String & engine_name_,
        const String & cluster_description,
        const String & remote_database_,
        const String & remote_table_,
        const String & username,
        const String & password,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

private:
    using Shards = std::unordered_set<StoragePtr>;
    Shards shards;
};

}

#endif
