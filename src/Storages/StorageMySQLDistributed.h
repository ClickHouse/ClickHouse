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

class StorageMySQLDistributed final : public ext::shared_ptr_helper<StorageMySQLDistributed>, public DB::IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQLDistributed>;

public:
    std::string getName() const override { return "MySQLDistributed"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageMySQLDistributed(
        const StorageID & table_id_,
        const String & remote_database__,
        const String & remote_table_,
        const String & cluster_description,
        const String & username,
        const String & password,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

private:
    using Replicas = std::shared_ptr<mysqlxx::PoolWithFailover>;
    using Shards = std::unordered_set<Replicas>;

    const std::string remote_database;
    const std::string remote_table;
    const Context & context;
    size_t shard_count;
    Shards shards;
};

}

#endif
