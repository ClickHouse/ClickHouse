#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>
#include <Disks/StoragePolicy.h>

#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/RemoteBlockOutputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/CurrentThread.h>
#include <Common/createHardLink.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include <ext/scope_guard.h>

#include <Poco/DirectoryIterator.h>

#include <future>
#include <condition_variable>
#include <mutex>


namespace CurrentMetrics
{
    extern const Metric DistributedSend;
}

namespace ProfileEvents
{
    extern const Event DistributedSyncInsertionTimeoutExceeded;
}

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

    ConvertingBlockInputStream convert(
        std::make_shared<OneBlockInputStream>(block),
        header,
        ConvertingBlockInputStream::MatchColumnsMode::Name);
    return convert.read();
}


static void writeBlockConvert(const BlockOutputStreamPtr & out, const Block & block, size_t repeats, Poco::Logger * log)
{
    Block adopted_block = adoptBlock(out->getHeader(), block, log);
    for (size_t i = 0; i < repeats; ++i)
        out->write(adopted_block);
}


DistributedBlockOutputStream::DistributedBlockOutputStream(
    ContextPtr context_,
    StorageDistributed & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ASTPtr & query_ast_,
    const ClusterPtr & cluster_,
    bool insert_sync_,
    UInt64 insert_timeout_,
    StorageID main_table_)
    : context(Context::createCopy(context_))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query_ast(query_ast_)
    , query_string(queryToString(query_ast_))
    , cluster(cluster_)
    , insert_sync(insert_sync_)
    , insert_timeout(insert_timeout_)
    , main_table(main_table_)
    , log(&Poco::Logger::get("DistributedBlockOutputStream"))
{
    const auto & settings = context->getSettingsRef();
    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth > settings.max_distributed_depth)
        throw Exception("Maximum distributed depth exceeded", ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH);
    context->getClientInfo().distributed_depth += 1;
}


Block DistributedBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void DistributedBlockOutputStream::writePrefix()
{
    storage.delayInsertOrThrowIfNeeded();
}


void DistributedBlockOutputStream::write(const Block & block)
{
    Block ordinary_block{ block };

    /* They are added by the AddingDefaultBlockOutputStream, and we will get
     * different number of columns eventually */
    for (const auto & col : metadata_snapshot->getColumns().getMaterialized())
    {
        if (ordinary_block.has(col.name))
        {
            ordinary_block.erase(col.name);
            LOG_DEBUG(log, "{}: column {} will be removed, because it is MATERIALIZED",
                storage.getStorageID().getNameForLogs(), col.name);
        }
    }


    if (insert_sync)
        writeSync(ordinary_block);
    else
        writeAsync(ordinary_block);
}

void DistributedBlockOutputStream::writeAsync(const Block & block)
{
    const Settings & settings = context->getSettingsRef();
    bool random_shard_insert = settings.insert_distributed_one_random_shard && !storage.has_sharding_key;

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


std::string DistributedBlockOutputStream::getCurrentStateDescription()
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


void DistributedBlockOutputStream::initWritingJobs(const Block & first_block, size_t start, size_t end)
{
    const Settings & settings = context->getSettingsRef();
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = end - start;

    remote_jobs_count = 0;
    local_jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : ext::range(start, end))
    {
        const auto & shard_info = shards_info[shard_index];
        auto & shard_jobs = per_shard_jobs[shard_index];

        /// If hasInternalReplication, than prefer local replica (if !prefer_localhost_replica)
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal() || !settings.prefer_localhost_replica)
        {
            const auto & replicas = addresses_with_failovers[shard_index];

            for (size_t replica_index : ext::range(0, replicas.size()))
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


void DistributedBlockOutputStream::waitForJobs()
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

    size_t jobs_count = remote_jobs_count + local_jobs_count;
    size_t num_finished_jobs = finished_jobs_count;

    if (num_finished_jobs < jobs_count)
        LOG_WARNING(log, "Expected {} writing jobs, but finished only {}", jobs_count, num_finished_jobs);
}


ThreadPool::Job
DistributedBlockOutputStream::runWritingJob(DistributedBlockOutputStream::JobReplica & job, const Block & current_block, size_t num_shards)
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
        if (shard_block.rows() == 0)
            return;

        if (!job.is_local_job || !settings.prefer_localhost_replica)
        {
            if (!job.stream)
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

                job.stream = std::make_shared<RemoteBlockOutputStream>(
                    *job.connection_entry, timeouts, query_string, settings, context->getClientInfo());
                job.stream->writePrefix();
            }

            CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

            Block adopted_shard_block = adoptBlock(job.stream->getHeader(), shard_block, log);
            job.stream->write(adopted_shard_block);
        }
        else // local
        {
            if (!job.stream)
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

                InterpreterInsertQuery interp(copy_query_ast, job.local_context);
                auto block_io = interp.execute();

                job.stream = block_io.out;
                job.stream->writePrefix();
            }

            writeBlockConvert(job.stream, shard_block, shard_info.getLocalNodeCount(), log);
        }

        job.blocks_written += 1;
        job.rows_written += shard_block.rows();
    };
}


void DistributedBlockOutputStream::writeSync(const Block & block)
{
    const Settings & settings = context->getSettingsRef();
    const auto & shards_info = cluster->getShardsInfo();
    bool random_shard_insert = settings.insert_distributed_one_random_shard && !storage.has_sharding_key;
    size_t start = 0;
    size_t end = shards_info.size();

    if (settings.insert_shard_id)
    {
        start = settings.insert_shard_id - 1;
        end = settings.insert_shard_id;
    }
    else if (random_shard_insert)
    {
        start = storage.getRandomShardIndex(shards_info);
        end = start + 1;
    }

    size_t num_shards = end - start;

    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        initWritingJobs(block, start, end);

        pool.emplace(remote_jobs_count + local_jobs_count);

        if (!throttler && (settings.max_network_bandwidth || settings.max_network_bytes))
        {
            throttler = std::make_shared<Throttler>(settings.max_network_bandwidth, settings.max_network_bytes,
                                                    "Network bandwidth limit for a query exceeded.");
        }

        watch.restart();
    }

    watch_current_block.restart();

    if (num_shards > 1)
    {
        auto current_selector = createSelector(block);

        /// Prepare row numbers for each shard
        for (size_t shard_index : ext::range(0, num_shards))
            per_shard_jobs[shard_index].shard_current_block_permutation.resize(0);

        for (size_t i = 0; i < block.rows(); ++i)
            per_shard_jobs[current_selector[i]].shard_current_block_permutation.push_back(i);
    }

    try
    {
        /// Run jobs in parallel for each block and wait them
        finished_jobs_count = 0;
        for (size_t shard_index : ext::range(0, shards_info.size()))
            for (JobReplica & job : per_shard_jobs[shard_index].replicas_jobs)
                pool->scheduleOrThrowOnError(runWritingJob(job, block, num_shards));
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
        throw;
    }

    inserted_blocks += 1;
    inserted_rows += block.rows();
}


void DistributedBlockOutputStream::writeSuffix()
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
                    if (job.stream)
                    {
                        pool->scheduleOrThrowOnError([&job]()
                        {
                            job.stream->writeSuffix();
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


IColumn::Selector DistributedBlockOutputStream::createSelector(const Block & source_block) const
{
    Block current_block_with_sharding_key_expr = source_block;
    storage.getShardingKeyExpr()->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(storage.getShardingKeyColumnName());

    return storage.createSelector(cluster, key_column);
}


Blocks DistributedBlockOutputStream::splitBlock(const Block & block)
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


void DistributedBlockOutputStream::writeSplitAsync(const Block & block)
{
    Blocks splitted_blocks = splitBlock(block);
    const size_t num_shards = splitted_blocks.size();

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        if (splitted_blocks[shard_idx].rows())
            writeAsyncImpl(splitted_blocks[shard_idx], shard_idx);

    ++inserted_blocks;
}


void DistributedBlockOutputStream::writeAsyncImpl(const Block & block, size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    const auto & settings = context->getSettingsRef();

    if (shard_info.hasInternalReplication())
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            /// Prefer insert into current instance directly
            writeToLocal(block, shard_info.getLocalNodeCount());
        else
        {
            const auto & path = shard_info.insertPathForInternalReplication(
                settings.prefer_localhost_replica,
                settings.use_compact_format_in_distributed_parts_names);
            if (path.empty())
                throw Exception("Directory name for async inserts is empty", ErrorCodes::LOGICAL_ERROR);
            writeToShard(block, {path});
        }
    }
    else
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            writeToLocal(block, shard_info.getLocalNodeCount());

        std::vector<std::string> dir_names;
        for (const auto & address : cluster->getShardsAddresses()[shard_id])
            if (!address.is_local || !settings.prefer_localhost_replica)
                dir_names.push_back(address.toFullString(settings.use_compact_format_in_distributed_parts_names));

        if (!dir_names.empty())
            writeToShard(block, dir_names);
    }
}


void DistributedBlockOutputStream::writeToLocal(const Block & block, size_t repeats)
{
    /// Async insert does not support settings forwarding yet whereas sync one supports
    InterpreterInsertQuery interp(query_ast, context);

    auto block_io = interp.execute();

    block_io.out->writePrefix();
    writeBlockConvert(block_io.out, block, repeats, log);
    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
    const auto & settings = context->getSettingsRef();
    const auto & distributed_settings = storage.getDistributedSettingsRef();

    bool fsync = distributed_settings.fsync_after_insert;
    bool dir_fsync = distributed_settings.fsync_directories;

    std::string compression_method = Poco::toUpper(settings.network_compression_method.toString());
    std::optional<int> compression_level;

    if (compression_method == "ZSTD")
        compression_level = settings.network_zstd_compression_level;

    CompressionCodecFactory::instance().validateCodec(compression_method, compression_level, !settings.allow_suspicious_codecs);
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
        Poco::File(path).createDirectory();

        const std::string tmp_path(path + "/tmp/");
        Poco::File(tmp_path).createDirectory();

        const std::string file_name(toString(storage.file_names_increment.get()) + ".bin");

        first_file_tmp_path = tmp_path + file_name;

        /// Write batch to temporary location
        {
            auto tmp_dir_sync_guard = make_directory_sync_guard(*it + "/tmp/");

            WriteBufferFromFile out{first_file_tmp_path};
            CompressedWriteBuffer compress{out, compression_codec};
            NativeBlockOutputStream stream{compress, DBMS_TCP_PROTOCOL_VERSION, block.cloneEmpty()};

            /// Prepare the header.
            /// See also readDistributedHeader() in DirectoryMonitor (for reading side)
            ///
            /// We wrap the header into a string for compatibility with older versions:
            /// a shard will able to read the header partly and ignore other parts based on its version.
            WriteBufferFromOwnString header_buf;
            writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, header_buf);
            writeStringBinary(query_string, header_buf);
            context->getSettingsRef().write(header_buf);
            context->getClientInfo().write(header_buf, DBMS_TCP_PROTOCOL_VERSION);
            writeVarUInt(block.rows(), header_buf);
            writeVarUInt(block.bytes(), header_buf);
            writeStringBinary(block.cloneEmpty().dumpStructure(), header_buf); /// obsolete
            /// Write block header separately in the batch header.
            /// It is required for checking does conversion is required or not.
            {
                NativeBlockOutputStream header_stream{header_buf, DBMS_TCP_PROTOCOL_VERSION, block.cloneEmpty()};
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

            stream.writePrefix();
            stream.write(block);
            stream.writeSuffix();

            out.finalize();
            if (fsync)
                out.sync();
        }

        // Create hardlink here to reuse increment number
        const std::string block_file_path(path + '/' + file_name);
        createHardLink(first_file_tmp_path, block_file_path);
        auto dir_sync_guard = make_directory_sync_guard(*it);
    }
    ++it;

    /// Make hardlinks
    for (; it != dir_names.end(); ++it)
    {
        const std::string path(disk_path + data_path + *it);
        Poco::File(path).createDirectory();

        const std::string block_file_path(path + '/' + toString(storage.file_names_increment.get()) + ".bin");
        createHardLink(first_file_tmp_path, block_file_path);
        auto dir_sync_guard = make_directory_sync_guard(*it);
    }

    auto file_size = Poco::File(first_file_tmp_path).getSize();
    /// remove the temporary file, enabling the OS to reclaim inode after all threads
    /// have removed their corresponding files
    Poco::File(first_file_tmp_path).remove();

    /// Notify
    auto sleep_ms = context->getSettingsRef().distributed_directory_monitor_sleep_time_ms;
    for (const auto & dir_name : dir_names)
    {
        auto & directory_monitor = storage.requireDirectoryMonitor(disk, dir_name);
        directory_monitor.addAndSchedule(file_size, sleep_ms.totalMilliseconds());
    }
}

}
