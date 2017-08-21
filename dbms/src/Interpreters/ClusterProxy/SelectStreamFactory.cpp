#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Exception.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{

extern const int ALL_REPLICAS_ARE_STALE;

}

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
        QueryProcessingStage::Enum processed_stage_,
        QualifiedTableName main_table_,
        const Tables & external_tables_)
    : processed_stage{processed_stage_}
    , main_table(std::move(main_table_))
    , external_tables{external_tables_}
{
}

namespace
{

BlockInputStreamPtr createLocalStream(const ASTPtr & query_ast, const Context & context, QueryProcessingStage::Enum processed_stage)
{
    InterpreterSelectQuery interpreter{query_ast, context, processed_stage};
    BlockInputStreamPtr stream = interpreter.execute().in;

    /** Materialization is needed, since from remote servers the constants come materialized.
      * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
      * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
      */
    return std::make_shared<MaterializingBlockInputStream>(stream);
}

}

void SelectStreamFactory::createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res)
{
    auto emplace_local_stream = [&]()
    {
        res.emplace_back(createLocalStream(query_ast, context, processed_stage));
    };

    auto emplace_remote_stream = [&]()
    {
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, context, nullptr, throttler, external_tables, processed_stage);
        stream->setPoolMode(PoolMode::GET_MANY);
        stream->setMainTable(main_table);
        res.emplace_back(std::move(stream));
    };

    if (shard_info.isLocal())
    {
        StoragePtr main_table_storage = context.tryGetTable(main_table.database, main_table.table);
        if (!main_table_storage) /// Table is absent on a local server.
        {
            if (shard_info.pool)
            {
                LOG_WARNING(
                        &Logger::get("ClusterProxy::SelectStreamFactory"),
                        "There is no table " << main_table.database << "." << main_table.table
                        << " on local replica of shard " << shard_info.shard_num << ", will try remote replicas.");

                emplace_remote_stream();
                return;
            }
            else
            {
                /// Let it fail the usual way.
                emplace_local_stream();
                return;
            }
        }

        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

        if (!replicated_storage)
        {
            /// Table is not replicated, use local server.
            emplace_local_stream();
            return;
        }

        const Settings & settings = context.getSettingsRef();
        UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

        if (!max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        UInt32 local_delay = replicated_storage->getAbsoluteDelay();

        if (local_delay < max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        /// If we reached this point, local replica is stale.

        if (!settings.fallback_to_stale_replicas_for_distributed_queries)
        {
            if (shard_info.pool)
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            else
                throw Exception(
                        "Local replica for shard " + toString(shard_info.shard_num)
                        + " is stale (delay: " + toString(local_delay) + "), but no other replica configured.",
                        ErrorCodes::ALL_REPLICAS_ARE_STALE);
        }

        if (!shard_info.pool)
        {
            /// There are no remote replicas but we are allowed to fall back to stale local replica.
            emplace_local_stream();
            return;
        }

        /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
        /// Do it lazily to avoid connecting in the main thread.

        auto lazily_create_stream = [
                pool = shard_info.pool, query, query_ast, context, throttler,
                main_table = main_table, external_tables = external_tables, stage = processed_stage,
                local_delay]()
            -> BlockInputStreamPtr
        {
            std::vector<ConnectionPoolWithFailover::TryResult> try_results =
                pool->getManyChecked(&context.getSettingsRef(), PoolMode::GET_MANY, main_table);

            double max_remote_delay = 0.0;
            for (const auto & try_result : try_results)
            {
                if (!try_result.is_up_to_date)
                    max_remote_delay = std::max(try_result.staleness, max_remote_delay);
            }

            if (local_delay < max_remote_delay)
                return createLocalStream(query_ast, context, stage);
            else
            {
                std::vector<IConnectionPool::Entry> connections;
                connections.reserve(try_results.size());
                for (auto & try_result : try_results)
                    connections.emplace_back(std::move(try_result.entry));

                return std::make_shared<RemoteBlockInputStream>(
                        std::move(connections), query, context, nullptr, throttler, external_tables, stage);
            }
        };

        res.emplace_back(std::make_shared<LazyBlockInputStream>("LazyShardWithLocalReplica", lazily_create_stream));
    }
    else
        emplace_remote_stream();
}

}
}
