#include <Storages/Distributed/DistributedSink.h>
#include <Storages/Distributed/DirectoryMonitor.h>
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
#include <IO/ConnectionTimeoutsContext.h>
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
#include <base/logger_useful.h>
#include <base/range.h>
#include <base/scope_guard.h>

#include <filesystem>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
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
}

static Block adoptBlock(const Block & header, const Block & block, Poco::Logger * log)
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


static void writeBlockConvert(PushingPipelineExecutor & executor, const Block & block, size_t repeats, Poco::Logger * log)
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
    StorageID main_table_,
    const Names & columns_to_send_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , context(Context::createCopy(context_))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query_ast(createInsertToRemoteTableQuery(main_table_.database_name, main_table_.table_name, columns_to_send_))
    , query_string(queryToString(query_ast))
    , cluster(cluster_)
    , insert_sync(insert_sync_)
    , allow_materialized(context->getSettingsRef().insert_allow_materialized_columns)
    , insert_timeout(insert_timeout_)
    , main_table(main_table_)
    , columns_to_send(columns_to_send_.begin(), columns_to_send_.end())
    , log(&Poco::Logger::get("DistributedBlockOutputStream"))
{
    const auto & settings = context->getSettingsRef();
    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
        throw Exception("Maximum distributed depth exceeded", ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH);
    context->getClientInfo().distributed_depth += 1;
    random_shard_insert = settings.insert_distributed_one_random_shard && !storage.has_sharding_key;
}


void DistributedSink::consume(Chunk chunk)
{
    if (is_first_chunk)
    {
        storage.delayInsertOrThrowIfNeeded();
        is_first_chunk = false;
    }

    auto ordinary_block = getHeader().cloneWithColumns(chunk.detachColumns());

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
            return writeSplitAsync(block);

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
            throw Exception("Synchronous distributed insert timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
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
        if (thread_group)
            CurrentThread::attachToIfDetached(thread_group);
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

        /// Do not initiate INSERT for empty block.
        size_t rows = shard_block.rows();
        if (rows == 0)
            return;

        OpenTelemetrySpanHolder span(__PRETTY_FUNCTION__);
        span.addAttribute("clickhouse.shard_num", shard_info.shard_num);
        span.addAttribute("clickhouse.written_rows", rows);

        if (!job.is_local_job || !settings.prefer_localhost_replica)
        {
            if (!job.executor)
            {
                auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
                if (shard_info.hasInternalReplication())
                {
                    /// Skip replica_index in case of internal replication
                    if (shard_job.replicas_jobs.size() != 1)
                        throw Exception("There are several writing job for an automatically replicated shard", ErrorCodes::LOGICAL_ERROR);

                    /// TODO: it make sense to rewrite skip_unavailable_shards and max_parallel_replicas here
                    auto results = shard_info.pool->getManyChecked(timeouts, &settings, PoolMode::GET_ONE, main_table.getQualifiedName());
                    if (results.empty() || results.front().entry.isNull())
                        throw Exception("Expected exactly one connection for shard " + toString(job.shard_index), ErrorCodes::LOGICAL_ERROR);

                    job.connection_entry = std::move(results.front().entry);
                }
                else
                {
                    const auto & replica = addresses.at(job.shard_index).at(job.replica_index);

                    const ConnectionPoolPtr & connection_pool = shard_info.per_replica_pools.at(job.replica_index);
                    if (!connection_pool)
                        throw Exception("Connection pool for replica " + replica.readableString() + " does not exist", ErrorCodes::LOGICAL_ERROR);

                    job.connection_entry = connection_pool->get(timeouts, &settings);
                    if (job.connection_entry.isNull())
                        throw Exception("Got empty connection for replica" + replica.readableString(), ErrorCodes::LOGICAL_ERROR);
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

                InterpreterInsertQuery interp(copy_query_ast, job.local_context, allow_materialized);
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
    OpenTelemetrySpanHolder span(__PRETTY_FUNCTION__);

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
        pool.emplace(/* max_threads_= */ max_threads,
                     /* max_free_threads_= */ max_threads,
                     /* queue_size_= */ jobs_count);

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
    span.addAttribute("db.statement", this->query_string);

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
                        pool->scheduleOrThrowOnError([&job]()
                        {
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


IColumn::Selector DistributedSink::createSelector(const Block & source_block) const
{
    Block current_block_with_sharding_key_expr = source_block;
    storage.getShardingKeyExpr()->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(storage.getShardingKeyColumnName());

    return storage.createSelector(cluster, key_column);
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
    OpenTelemetrySpanHolder span("DistributedBlockOutputStream::writeAsyncImpl()");

    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    const auto & settings = context->getSettingsRef();
    Block block_to_send = removeSuperfluousColumns(block);

    span.addAttribute("clickhouse.shard_num", shard_info.shard_num);
    span.addAttribute("clickhouse.written_rows", block.rows());

    if (shard_info.hasInternalReplication())
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            /// Prefer insert into current instance directly
            writeToLocal(block_to_send, shard_info.getLocalNodeCount());
        else
        {
            const auto & path = shard_info.insertPathForInternalReplication(
                settings.prefer_localhost_replica,
                settings.use_compact_format_in_distributed_parts_names);
            if (path.empty())
                throw Exception("Directory name for async inserts is empty", ErrorCodes::LOGICAL_ERROR);
            writeToShard(block_to_send, {path});
        }
    }
    else
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            writeToLocal(block_to_send, shard_info.getLocalNodeCount());

        std::vector<std::string> dir_names;
        for (const auto & address : cluster->getShardsAddresses()[shard_id])
            if (!address.is_local || !settings.prefer_localhost_replica)
                dir_names.push_back(address.toFullString(settings.use_compact_format_in_distributed_parts_names));

        if (!dir_names.empty())
            writeToShard(block_to_send, dir_names);
    }
}


void DistributedSink::writeToLocal(const Block & block, size_t repeats)
{
    OpenTelemetrySpanHolder span(__PRETTY_FUNCTION__);
    span.addAttribute("db.statement", this->query_string);

    InterpreterInsertQuery interp(query_ast, context, allow_materialized);

    auto block_io = interp.execute();
    PushingPipelineExecutor executor(block_io.pipeline);

    executor.start();
    writeBlockConvert(executor, block, repeats, log);
    executor.finish();
}


void DistributedSink::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
    OpenTelemetrySpanHolder span(__PRETTY_FUNCTION__);

    const auto & settings = context->getSettingsRef();
    const auto & distributed_settings = storage.getDistributedSettingsRef();

    bool fsync = distributed_settings.fsync_after_insert;
    bool dir_fsync = distributed_settings.fsync_directories;

    std::string compression_method = Poco::toUpper(settings.network_compression_method.toString());
    std::optional<int> compression_level;

    if (compression_method == "ZSTD")
        compression_level = settings.network_zstd_compression_level;

    CompressionCodecFactory::instance().validateCodec(compression_method, compression_level, !settings.allow_suspicious_codecs, settings.allow_experimental_codecs);
    CompressionCodecPtr compression_codec = CompressionCodecFactory::instance().get(compression_method, compression_level);

    /// tmp directory is used to ensure atomicity of transactions
    /// and keep monitor thread out from reading incomplete data
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
            /// See also readDistributedHeader() in DirectoryMonitor (for reading side)
            ///
            /// We wrap the header into a string for compatibility with older versions:
            /// a shard will able to read the header partly and ignore other parts based on its version.
            WriteBufferFromOwnString header_buf;
            writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, header_buf);
            writeStringBinary(query_string, header_buf);
            context->getSettingsRef().write(header_buf);

            if (context->getClientInfo().client_trace_context.trace_id != UUID() && CurrentThread::isInitialized())
            {
                // if the distributed tracing is enabled, use the trace context in current thread as parent of next span
                auto client_info = context->getClientInfo();
                client_info.client_trace_context = CurrentThread::get().thread_trace_context;
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

            /// Add new fields here, for example:
            /// writeVarUInt(my_new_data, header_buf);
            /// And note that it is safe, because we have checksum and size for header.

            /// Write the header.
            const StringRef header = header_buf.stringRef();
            writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER, out);
            writeStringBinary(header, out);
            writePODBinary(CityHash_v1_0_2::CityHash128(header.data, header.size), out);

            stream.write(block);

            compress.finalize();
            out.finalize();
            if (fsync)
                out.sync();
        }

        // Create hardlink here to reuse increment number
        const std::string block_file_path(fs::path(path) / file_name);
        createHardLink(first_file_tmp_path, block_file_path);
        auto dir_sync_guard = make_directory_sync_guard(*it);
    }
    ++it;

    /// Make hardlinks
    for (; it != dir_names.end(); ++it)
    {
        const std::string path(fs::path(disk_path) / (data_path + *it));
        fs::create_directory(path);

        const std::string block_file_path(fs::path(path) / (toString(storage.file_names_increment.get()) + ".bin"));
        createHardLink(first_file_tmp_path, block_file_path);
        auto dir_sync_guard = make_directory_sync_guard(*it);
    }

    auto file_size = fs::file_size(first_file_tmp_path);
    /// remove the temporary file, enabling the OS to reclaim inode after all threads
    /// have removed their corresponding files
    fs::remove(first_file_tmp_path);

    /// Notify
    auto sleep_ms = context->getSettingsRef().distributed_directory_monitor_sleep_time_ms;
    for (const auto & dir_name : dir_names)
    {
        auto & directory_monitor = storage.requireDirectoryMonitor(disk, dir_name, /* startup= */ false);
        directory_monitor.addAndSchedule(file_size, sleep_ms.totalMilliseconds());
    }
}

}
