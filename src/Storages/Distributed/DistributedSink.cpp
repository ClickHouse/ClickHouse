#include <Storages/Distributed/DistributedSink.h>
#include <Storages/Distributed/DistributedAsyncInsertDirectoryQueue.h>
#include <Storages/Distributed/Defines.h>
#include <Storages/StorageDistributed.h>
#include <Disks/StoragePolicy.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ConnectionTimeouts.h>
#include <Formats/NativeWriter.h>
#include <Processors/Sinks/RemoteSink.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/CurrentThread.h>
#include <Common/createHardLink.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Core/Settings.h>

#include <filesystem>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
    extern const Metric DistributedInsertThreads;
    extern const Metric DistributedInsertThreadsActive;
    extern const Metric DistributedInsertThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event DistributedSyncInsertionTimeoutExceeded;
}

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int ABORTED;
}

static Block adoptBlock(const Block & header, const Block & block, LoggerPtr log)
{
    if (blocksHaveEqualStructure(header, block))
        return block;

    LOG_WARNING(log,
        "Structure does not match (remote: {}, local: {}), implicit conversion will be done.",
        header.dumpStructure(), block.dumpStructure());

    auto converting_dag = ActionsDAG::makeConvertingActions(
        block.cloneEmpty().getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    Block converted = block;
    converting_actions->execute(converted);

    return converted;
}


static void writeBlockConvert(PushingPipelineExecutor & executor, const Block & block, size_t repeats, LoggerPtr log)
{
    Block adopted_block = adoptBlock(executor.getHeader(), block, log);
    for (size_t i = 0; i < repeats; ++i)
        executor.push(adopted_block);
}


static ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Names & column_names)
{
    auto query = std::make_shared<ASTInsertQuery>();
    query->table_id = StorageID(database, table);
    auto columns = std::make_shared<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & column_name : column_names)
        columns->children.push_back(std::make_shared<ASTIdentifier>(column_name));
    return query;
}


DistributedSink::DistributedSink(
    ContextPtr context_,
    StorageDistributed & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ClusterPtr & cluster_,
    bool insert_sync_,
    UInt64 insert_timeout_,
    const Names & columns_to_send_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , context(Context::createCopy(context_))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query_ast(createInsertToRemoteTableQuery(storage.remote_storage.database_name, storage.remote_storage.table_name, columns_to_send_))
    , query_string(queryToString(query_ast))
    , cluster(cluster_)
    , insert_sync(insert_sync_)
    , allow_materialized(context->getSettingsRef().insert_allow_materialized_columns)
    , insert_timeout(insert_timeout_)
    , columns_to_send(columns_to_send_.begin(), columns_to_send_.end())
    , log(getLogger("DistributedSink"))
{
    const auto & settings = context->getSettingsRef();
    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");
    context->increaseDistributedDepth();
    random_shard_insert = settings.insert_distributed_one_random_shard && !storage.has_sharding_key;
}


void DistributedSink::consume(Chunk & chunk)
{
    if (is_first_chunk)
    {
        storage.delayInsertOrThrowIfNeeded();
        is_first_chunk = false;
    }

    auto ordinary_block = getHeader().cloneWithColumns(chunk.getColumns());

    if (insert_sync)
        writeSync(ordinary_block);
    else
        writeAsync(ordinary_block);
}


Block DistributedSink::removeSuperfluousColumns(Block block) const
{
    for (size_t i = block.columns(); i;)
    {
        --i;
        if (!columns_to_send.contains(block.getByPosition(i).name))
            block.erase(i);
    }
    return block;
}


void DistributedSink::writeAsync(const Block & block)
{
    if (random_shard_insert)
    {
        writeAsyncImpl(block, storage.getRandomShardIndex(cluster->getShardsInfo()));
        ++inserted_blocks;
    }
    else
    {
        if (storage.getShardingKeyExpr() && (cluster->getShardsInfo().size() > 1))
        {
            writeSplitAsync(block);
            return;
        }

        writeAsyncImpl(block);
        ++inserted_blocks;
    }
}


std::string DistributedSink::getCurrentStateDescription()
{
    WriteBufferFromOwnString buffer;
    const auto & addresses = cluster->getShardsAddresses();

    buffer << "Insertion status:\n";
    for (auto & shard_jobs : per_shard_jobs)
        for (JobReplica & job : shard_jobs.replicas_jobs)
        {
            buffer << "Wrote " << job.blocks_written << " blocks and " << job.rows_written << " rows"
                   << " on shard " << job.shard_index << " replica " << job.replica_index
                   << ", " << addresses[job.shard_index][job.replica_index].readableString();

            /// Performance statistics
            if (job.blocks_started > 0)
            {
                buffer << " (average " << job.elapsed_time_ms / job.blocks_started << " ms per block";
                if (job.blocks_started > 1)
                    buffer << ", the slowest block " << job.max_elapsed_time_for_block_ms << " ms";
                buffer << ")";
            }

            buffer << "\n";
        }

    return buffer.str();
}


DistributedSink::JobReplica::JobReplica(size_t shard_index_, size_t replica_index_, bool is_local_job_, const Block & sample_block)
    : shard_index(shard_index_), replica_index(replica_index_), is_local_job(is_local_job_), current_shard_block(sample_block.cloneEmpty()) {}


void DistributedSink::initWritingJobs(const Block & first_block, size_t start, size_t end)
{
    const Settings & settings = context->getSettingsRef();
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = end - start;

    remote_jobs_count = 0;
    local_jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : collections::range(start, end))
    {
        const auto & shard_info = shards_info[shard_index];
        auto & shard_jobs = per_shard_jobs[shard_index];

        /// If hasInternalReplication, than prefer local replica (if !prefer_localhost_replica)
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal() || !settings.prefer_localhost_replica)
        {
            const auto & replicas = addresses_with_failovers[shard_index];

            for (size_t replica_index : collections::range(0, replicas.size()))
            {
                if (!replicas[replica_index].is_local || !settings.prefer_localhost_replica)
                {
                    shard_jobs.replicas_jobs.emplace_back(shard_index, replica_index, false, first_block);
                    ++remote_jobs_count;

                    if (shard_info.hasInternalReplication())
                        break;
                }
            }
        }

        if (shard_info.isLocal() && settings.prefer_localhost_replica)
        {
            shard_jobs.replicas_jobs.emplace_back(shard_index, 0, true, first_block);
            ++local_jobs_count;
        }

        if (num_shards > 1)
            shard_jobs.shard_current_block_permutation.reserve(first_block.rows());
    }
}


void DistributedSink::waitForJobs()
{
    pool->wait();

    if (insert_timeout)
    {
        if (static_cast<UInt64>(watch.elapsedSeconds()) > insert_timeout)
        {
            ProfileEvents::increment(ProfileEvents::DistributedSyncInsertionTimeoutExceeded);
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Synchronous distributed insert timeout exceeded.");
        }
    }

    size_t num_finished_jobs = finished_jobs_count;
    if (random_shard_insert)
    {
        if (finished_jobs_count != 1)
            LOG_WARNING(log, "Expected 1 writing jobs when doing random shard insert, but finished {}", num_finished_jobs);
    }
    else
    {
        size_t jobs_count = remote_jobs_count + local_jobs_count;

        if (num_finished_jobs < jobs_count)
            LOG_WARNING(log, "Expected {} writing jobs, but finished only {}", jobs_count, num_finished_jobs);
    }
}


ThreadPool::Job
DistributedSink::runWritingJob(JobReplica & job, const Block & current_block, size_t num_shards)
{
    auto thread_group = CurrentThread::getGroup();
    return [this, thread_group, &job, &current_block, num_shards]()
    {
        /// Avoid Logical error: 'Pipeline for PushingPipelineExecutor was finished before all data was inserted' (whatever it means)
        if (isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Writing job was cancelled");

        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
        );
        OpenTelemetry::SpanHolder span(__PRETTY_FUNCTION__);

        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);
        setThreadName("DistrOutStrProc");

        ++job.blocks_started;

        SCOPE_EXIT({
            ++finished_jobs_count;

            UInt64 elapsed_time_for_block_ms = watch_current_block.elapsedMilliseconds();
            job.elapsed_time_ms += elapsed_time_for_block_ms;
            job.max_elapsed_time_for_block_ms = std::max(job.max_elapsed_time_for_block_ms, elapsed_time_for_block_ms);
        });

        const auto & shard_info = cluster->getShardsInfo()[job.shard_index];
        auto & shard_job = per_shard_jobs[job.shard_index];
        const auto & addresses = cluster->getShardsAddresses();

        /// Generate current shard block
        if (num_shards > 1)
        {
            auto & shard_permutation = shard_job.shard_current_block_permutation;
            size_t num_shard_rows = shard_permutation.size();

            for (size_t j = 0; j < current_block.columns(); ++j)
            {
                const auto & src_column = current_block.getByPosition(j).column;
                auto & dst_column = job.current_shard_block.getByPosition(j).column;

                /// Zero permutation size has special meaning in IColumn::permute
                if (num_shard_rows)
                    dst_column = src_column->permute(shard_permutation, num_shard_rows);
                else
                    dst_column = src_column->cloneEmpty();
            }
        }

        const Block & shard_block = (num_shards > 1) ? job.current_shard_block : current_block;
        const Settings & settings = context->getSettingsRef();

        size_t rows = shard_block.rows();

        span.addAttribute("clickhouse.shard_num", shard_info.shard_num);
        span.addAttribute("clickhouse.cluster", storage.cluster_name);
        span.addAttribute("clickhouse.distributed", storage.getStorageID().getFullNameNotQuoted());
        span.addAttribute("clickhouse.remote", [this]() { return storage.getRemoteDatabaseName() + "." + storage.getRemoteTableName(); });
        span.addAttribute("clickhouse.rows", rows);
        span.addAttribute("clickhouse.bytes", [&shard_block]() { return toString(shard_block.bytes()); });

        /// Do not initiate INSERT for empty block.
        if (rows == 0)
            return;

        if (!job.is_local_job || !settings.prefer_localhost_replica)
        {
            if (!job.executor)
            {
                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
                if (shard_info.hasInternalReplication())
                {
                    /// Skip replica_index in case of internal replication
                    if (shard_job.replicas_jobs.size() != 1)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are several writing job for an automatically replicated shard");

                    /// TODO: it make sense to rewrite skip_unavailable_shards and max_parallel_replicas here
                    /// NOTE: INSERT will also take into account max_replica_delay_for_distributed_queries
                    /// (anyway fallback_to_stale_replicas_for_distributed_queries=true by default)
                    auto results = shard_info.pool->getManyCheckedForInsert(timeouts, settings, PoolMode::GET_ONE, storage.remote_storage.getQualifiedName());
                    auto result = results.front();
                    if (shard_info.pool->isTryResultInvalid(result, settings.distributed_insert_skip_read_only_replicas))
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got an invalid connection result");

                    job.connection_entry = std::move(result.entry);
                }
                else
                {
                    const auto & replica = addresses.at(job.shard_index).at(job.replica_index);

                    const ConnectionPoolPtr & connection_pool = shard_info.per_replica_pools.at(job.replica_index);
                    if (!connection_pool)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Connection pool for replica {} does not exist", replica.readableString());

                    job.connection_entry = connection_pool->get(timeouts, settings);
                    if (job.connection_entry.isNull())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty connection for replica{}", replica.readableString());
                }

                if (throttler)
                    job.connection_entry->setThrottler(throttler);

                job.pipeline = QueryPipeline(std::make_shared<RemoteSink>(
                    *job.connection_entry, timeouts, query_string, settings, context->getClientInfo()));
                job.executor = std::make_unique<PushingPipelineExecutor>(job.pipeline);
                job.executor->start();
            }

            CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

            Block adopted_shard_block = adoptBlock(job.executor->getHeader(), shard_block, log);
            job.executor->push(adopted_shard_block);
        }
        else // local
        {
            if (!job.executor)
            {
                /// Forward user settings
                job.local_context = Context::createCopy(context);

                /// Copying of the query AST is required to avoid race,
                /// in case of INSERT into multiple local shards.
                ///
                /// Since INSERT into local node uses AST,
                /// and InterpreterInsertQuery::execute() is modifying it,
                /// to resolve tables (in InterpreterInsertQuery::getTable())
                auto copy_query_ast = query_ast->clone();

                InterpreterInsertQuery interp(
                    copy_query_ast,
                    job.local_context,
                    allow_materialized,
                    /* no_squash */ false,
                    /* no_destination */ false,
                    /* async_isnert */ false);
                auto block_io = interp.execute();

                job.pipeline = std::move(block_io.pipeline);
                job.executor = std::make_unique<PushingPipelineExecutor>(job.pipeline);
                job.executor->start();
            }

            writeBlockConvert(*job.executor, shard_block, shard_info.getLocalNodeCount(), log);
        }

        job.blocks_written += 1;
        job.rows_written += rows;
    };
}


void DistributedSink::writeSync(const Block & block)
{
    std::lock_guard lock(execution_mutex);
    if (isCancelled())
        return;

    OpenTelemetry::SpanHolder span(__PRETTY_FUNCTION__);

    const Settings & settings = context->getSettingsRef();
    const auto & shards_info = cluster->getShardsInfo();
    Block block_to_send = removeSuperfluousColumns(block);

    size_t start = 0;
    size_t end = shards_info.size();

    if (settings.insert_shard_id)
    {
        start = settings.insert_shard_id - 1;
        end = settings.insert_shard_id;
    }

    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        initWritingJobs(block_to_send, start, end);

        size_t jobs_count = random_shard_insert ? 1 : (remote_jobs_count + local_jobs_count);
        size_t max_threads = std::min<size_t>(settings.max_distributed_connections, jobs_count);
        pool.emplace(
            CurrentMetrics::DistributedInsertThreads,
            CurrentMetrics::DistributedInsertThreadsActive,
            CurrentMetrics::DistributedInsertThreadsScheduled,
            max_threads, max_threads, jobs_count);

        if (!throttler && (settings.max_network_bandwidth || settings.max_network_bytes))
        {
            throttler = std::make_shared<Throttler>(settings.max_network_bandwidth, settings.max_network_bytes,
                                                    "Network bandwidth limit for a query exceeded.");
        }

        watch.restart();
    }

    watch_current_block.restart();

    if (random_shard_insert)
    {
        start = storage.getRandomShardIndex(shards_info);
        end = start + 1;
    }

    size_t num_shards = end - start;

    span.addAttribute("clickhouse.start_shard", start);
    span.addAttribute("clickhouse.end_shard", end);
    span.addAttribute("db.statement", query_string);

    if (num_shards > 1)
    {
        auto current_selector = createSelector(block);

        /// Prepare row numbers for needed shards
        for (size_t shard_index : collections::range(start, end))
            per_shard_jobs[shard_index].shard_current_block_permutation.resize(0);

        for (size_t i = 0; i < block.rows(); ++i)
            per_shard_jobs[current_selector[i]].shard_current_block_permutation.push_back(i);
    }

    try
    {
        /// Run jobs in parallel for each block and wait them
        finished_jobs_count = 0;
        for (size_t shard_index : collections::range(start, end))
            for (JobReplica & job : per_shard_jobs[shard_index].replicas_jobs)
                pool->scheduleOrThrowOnError(runWritingJob(job, block_to_send, num_shards));
    }
    catch (...)
    {
        pool->wait();
        throw;
    }

    try
    {
        waitForJobs();
    }
    catch (Exception & exception)
    {
        exception.addMessage(getCurrentStateDescription());
        span.addAttribute(exception);
        throw;
    }

    inserted_blocks += 1;
    inserted_rows += block.rows();
}


void DistributedSink::onFinish()
{
    auto log_performance = [this]()
    {
        double elapsed = watch.elapsedSeconds();
        LOG_DEBUG(log, "It took {} sec. to insert {} blocks, {} rows per second. {}", elapsed, inserted_blocks, inserted_rows / elapsed, getCurrentStateDescription());
    };

    std::lock_guard lock(execution_mutex);
    if (isCancelled())
        return;

    /// Pool finished means that some exception had been thrown before,
    /// and scheduling new jobs will return "Cannot schedule a task" error.
    if (insert_sync && pool && !pool->finished())
    {
        finished_jobs_count = 0;
        try
        {
            for (auto & shard_jobs : per_shard_jobs)
            {
                for (JobReplica & job : shard_jobs.replicas_jobs)
                {
                    if (job.executor)
                    {
                        pool->scheduleOrThrowOnError([&job, thread_group = CurrentThread::getGroup()]()
                        {
                            SCOPE_EXIT_SAFE(
                                if (thread_group)
                                    CurrentThread::detachFromGroupIfNotDetached();
                            );
                            if (thread_group)
                                CurrentThread::attachToGroupIfDetached(thread_group);

                            job.executor->finish();
                        });
                    }
                }
            }
        }
        catch (...)
        {
            pool->wait();
            throw;
        }

        try
        {
            pool->wait();
            log_performance();
        }
        catch (Exception & exception)
        {
            log_performance();
            exception.addMessage(getCurrentStateDescription());
            throw;
        }
    }
}

void DistributedSink::onCancel() noexcept
{
    std::lock_guard lock(execution_mutex);
    if (pool && !pool->finished())
    {
        try
        {
            pool->wait();
        }
        catch (...)
        {
            tryLogCurrentException(storage.log, "Error occurs on cancellation.");
        }
    }

    for (auto & shard_jobs : per_shard_jobs)
    {
        for (JobReplica & job : shard_jobs.replicas_jobs)
        {
            try
            {
                if (job.executor)
                    job.executor->cancel();
            }
            catch (...)
            {
                tryLogCurrentException(storage.log, "Error occurs on cancellation.");
            }
        }
    }

}


IColumn::Selector DistributedSink::createSelector(const Block & source_block) const
{
    Block current_block_with_sharding_key_expr = source_block;
    storage.getShardingKeyExpr()->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(storage.getShardingKeyColumnName());

    return StorageDistributed::createSelector(cluster, key_column);
}


Blocks DistributedSink::splitBlock(const Block & block)
{
    auto selector = createSelector(block);

    /// Split block to num_shard smaller block, using 'selector'.

    const size_t num_shards = cluster->getShardsInfo().size();
    Blocks splitted_blocks(num_shards);

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        splitted_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_shards, selector);
        for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
            splitted_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(splitted_columns[shard_idx]);
    }

    return splitted_blocks;
}


void DistributedSink::writeSplitAsync(const Block & block)
{
    Blocks splitted_blocks = splitBlock(block);
    const size_t num_shards = splitted_blocks.size();

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        if (splitted_blocks[shard_idx].rows())
            writeAsyncImpl(splitted_blocks[shard_idx], shard_idx);

    ++inserted_blocks;
}


void DistributedSink::writeAsyncImpl(const Block & block, size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    const auto & settings = context->getSettingsRef();
    Block block_to_send = removeSuperfluousColumns(block);

    if (shard_info.hasInternalReplication())
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            /// Prefer insert into current instance directly
            writeToLocal(shard_info, block_to_send, shard_info.getLocalNodeCount());
        else
        {
            const auto & path = shard_info.insertPathForInternalReplication(
                settings.prefer_localhost_replica,
                settings.use_compact_format_in_distributed_parts_names);
            if (path.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory name for async inserts is empty");
            writeToShard(shard_info, block_to_send, {path});
        }
    }
    else
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            writeToLocal(shard_info, block_to_send, shard_info.getLocalNodeCount());

        std::vector<std::string> dir_names;
        for (const auto & address : cluster->getShardsAddresses()[shard_id])
            if (!address.is_local || !settings.prefer_localhost_replica)
                dir_names.push_back(address.toFullString(settings.use_compact_format_in_distributed_parts_names));

        if (!dir_names.empty())
            writeToShard(shard_info, block_to_send, dir_names);
    }
}


void DistributedSink::writeToLocal(const Cluster::ShardInfo & shard_info, const Block & block, size_t repeats)
{
    OpenTelemetry::SpanHolder span(__PRETTY_FUNCTION__);
    span.addAttribute("clickhouse.shard_num", shard_info.shard_num);
    span.addAttribute("clickhouse.cluster", storage.cluster_name);
    span.addAttribute("clickhouse.distributed", storage.getStorageID().getFullNameNotQuoted());
    span.addAttribute("clickhouse.remote", [this]() { return storage.getRemoteDatabaseName() + "." + storage.getRemoteTableName(); });
    span.addAttribute("clickhouse.rows", [&block]() { return toString(block.rows()); });
    span.addAttribute("clickhouse.bytes", [&block]() { return toString(block.bytes()); });

    try
    {
        InterpreterInsertQuery interp(
            query_ast,
            context,
            allow_materialized,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false);

        auto block_io = interp.execute();
        PushingPipelineExecutor executor(block_io.pipeline);

        executor.start();
        writeBlockConvert(executor, block, repeats, log);
        executor.finish();
    }
    catch (...)
    {
        span.addAttribute(std::current_exception());
        throw;
    }
}


void DistributedSink::writeToShard(const Cluster::ShardInfo & shard_info, const Block & block, const std::vector<std::string> & dir_names)
{
    OpenTelemetry::SpanHolder span(__PRETTY_FUNCTION__);
    span.addAttribute("clickhouse.shard_num", shard_info.shard_num);

    const auto & settings = context->getSettingsRef();
    const auto & distributed_settings = storage.getDistributedSettingsRef();

    bool fsync = distributed_settings.fsync_after_insert;
    bool dir_fsync = distributed_settings.fsync_directories;

    std::string compression_method = Poco::toUpper(settings.network_compression_method.toString());
    std::optional<int> compression_level;

    if (compression_method == "ZSTD")
        compression_level = settings.network_zstd_compression_level;

    CompressionCodecFactory::instance().validateCodec(compression_method, compression_level, !settings.allow_suspicious_codecs, settings.allow_experimental_codecs, settings.enable_deflate_qpl_codec, settings.enable_zstd_qat_codec);
    CompressionCodecPtr compression_codec = CompressionCodecFactory::instance().get(compression_method, compression_level);

    /// tmp directory is used to ensure atomicity of transactions
    /// and keep directory queue thread out from reading incomplete data
    std::string first_file_tmp_path;

    auto reservation = storage.getStoragePolicy()->reserveAndCheck(block.bytes());
    const auto disk = reservation->getDisk();
    auto disk_path = disk->getPath();
    auto data_path = storage.getRelativeDataPath();

    auto make_directory_sync_guard = [&](const std::string & current_path)
    {
        SyncGuardPtr guard;
        if (dir_fsync)
        {
            const std::string relative_path(data_path + current_path);
            guard = disk->getDirectorySyncGuard(relative_path);
        }
        return guard;
    };

    auto sleep_ms = context->getSettingsRef().distributed_background_insert_sleep_time_ms.totalMilliseconds();
    size_t file_size;

    auto it = dir_names.begin();
    /// on first iteration write block to a temporary directory for subsequent
    /// hardlinking to ensure the inode is not freed until we're done
    {
        const std::string path(disk_path + data_path + *it);
        const std::string tmp_path(path + "/tmp/");

        fs::create_directory(path);
        fs::create_directory(tmp_path);

        const std::string file_name(toString(storage.file_names_increment.get()) + ".bin");

        first_file_tmp_path = tmp_path + file_name;

        /// Write batch to temporary location
        {
            auto tmp_dir_sync_guard = make_directory_sync_guard(*it + "/tmp/");

            WriteBufferFromFile out{first_file_tmp_path};
            CompressedWriteBuffer compress{out, compression_codec};
            NativeWriter stream{compress, DBMS_TCP_PROTOCOL_VERSION, block.cloneEmpty()};

            /// Prepare the header.
            /// See also DistributedAsyncInsertHeader::read() in DistributedInsertQueue (for reading side)
            ///
            /// We wrap the header into a string for compatibility with older versions:
            /// a shard will able to read the header partly and ignore other parts based on its version.
            WriteBufferFromOwnString header_buf;
            writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, header_buf);
            writeStringBinary(query_string, header_buf);
            context->getSettingsRef().write(header_buf);

            if (OpenTelemetry::CurrentContext().isTraceEnabled())
            {
                // if the distributed tracing is enabled, use the trace context in current thread as parent of next span
                auto client_info = context->getClientInfo();
                client_info.client_trace_context = OpenTelemetry::CurrentContext();
                client_info.write(header_buf, DBMS_TCP_PROTOCOL_VERSION);
            }
            else
            {
                context->getClientInfo().write(header_buf, DBMS_TCP_PROTOCOL_VERSION);
            }

            writeVarUInt(block.rows(), header_buf);
            writeVarUInt(block.bytes(), header_buf);
            writeStringBinary(block.cloneEmpty().dumpStructure(), header_buf); /// obsolete
            /// Write block header separately in the batch header.
            /// It is required for checking does conversion is required or not.
            {
                NativeWriter header_stream{header_buf, DBMS_TCP_PROTOCOL_VERSION, block.cloneEmpty()};
                header_stream.write(block.cloneEmpty());
            }

            writeVarUInt(shard_info.shard_num, header_buf);
            writeStringBinary(storage.cluster_name, header_buf);
            writeStringBinary(storage.getStorageID().getFullNameNotQuoted(), header_buf);
            writeStringBinary(storage.getRemoteDatabaseName() + "." + storage.getRemoteTableName(), header_buf);

            /// Add new fields here, for example:
            /// writeVarUInt(my_new_data, header_buf);
            /// And note that it is safe, because we have checksum and size for header.

            /// Write the header.
            const std::string_view header = header_buf.stringView();
            writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER, out);
            writeStringBinary(header, out);
            writePODBinary(CityHash_v1_0_2::CityHash128(header.data(), header.size()), out);

            stream.write(block);

            compress.finalize();
            out.finalize();
            if (fsync)
                out.sync();
        }

        file_size = fs::file_size(first_file_tmp_path);

        // Create hardlink here to reuse increment number
        auto bin_file = (fs::path(path) / file_name).string();
        auto & directory_queue = storage.getDirectoryQueue(disk, *it);
        {
            createHardLink(first_file_tmp_path, bin_file);
            auto dir_sync_guard = make_directory_sync_guard(*it);
        }
        directory_queue.addFileAndSchedule(bin_file, file_size, sleep_ms);
    }
    ++it;

    /// Make hardlinks
    for (; it != dir_names.end(); ++it)
    {
        const std::string path(fs::path(disk_path) / (data_path + *it));
        fs::create_directory(path);

        auto bin_file = (fs::path(path) / (toString(storage.file_names_increment.get()) + ".bin")).string();
        auto & directory_queue = storage.getDirectoryQueue(disk, *it);
        {
            createHardLink(first_file_tmp_path, bin_file);
            auto dir_sync_guard = make_directory_sync_guard(*it);
        }
        directory_queue.addFileAndSchedule(bin_file, file_size, sleep_ms);
    }

    /// remove the temporary file, enabling the OS to reclaim inode after all threads
    /// have removed their corresponding files
    fs::remove(first_file_tmp_path);
}

}
