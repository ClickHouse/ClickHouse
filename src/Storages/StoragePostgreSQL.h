#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace Poco
{
class Logger;
}

namespace postgres
{
class PoolWithFailover;
using PoolWithFailoverPtr = std::shared_ptr<PoolWithFailover>;
}

namespace DB
{
class NamedCollection;

class StoragePostgreSQL final : public IStorage
{
public:
    StoragePostgreSQL(
        const StorageID & table_id_,
        postgres::PoolWithFailoverPtr pool_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & remote_table_schema_ = "",
        const String & on_conflict = "");

    String getName() const override { return "PostgreSQL"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    struct Configuration
    {
        String host;
        UInt16 port = 0;
        String username = "default";
        String password;
        String database;
        String table;
        String schema;
        String on_conflict;

        std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
        String addresses_expr;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    static Configuration processNamedCollectionResult(const NamedCollection & named_collection, bool require_table = true);

private:
    String remote_table_name;
    String remote_table_schema;
    String on_conflict;
    postgres::PoolWithFailoverPtr pool;

    Poco::Logger * log;
};

}

#endif
