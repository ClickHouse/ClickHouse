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
#include <Common/ClickHouseRevision.h>
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
}

static void writeBlockConvert(const BlockOutputStreamPtr & out, const Block & block, const size_t repeats)
{
    if (!blocksHaveEqualStructure(out->getHeader(), block))
    {
        ConvertingBlockInputStream convert(
            std::make_shared<OneBlockInputStream>(block),
            out->getHeader(),
            ConvertingBlockInputStream::MatchColumnsMode::Name);
        auto adopted_block = convert.read();

        for (size_t i = 0; i < repeats; ++i)
            out->write(adopted_block);
    }
    else
    {
        for (size_t i = 0; i < repeats; ++i)
            out->write(block);
    }
}


DistributedBlockOutputStream::DistributedBlockOutputStream(
    const Context & context_,
    StorageDistributed & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ASTPtr & query_ast_,
    const ClusterPtr & cluster_,
    bool insert_sync_,
    UInt64 insert_timeout_)
    : context(context_)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , query_ast(query_ast_)
    , query_string(queryToString(query_ast_))
    , cluster(cluster_)
    , insert_sync(insert_sync_)
    , insert_timeout(insert_timeout_)
    , log(&Poco::Logger::get("DistributedBlockOutputStream"))
{
}


Block DistributedBlockOutputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlock();
}


void DistributedBlockOutputStream::writePrefix()
{
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
    if (storage.getShardingKeyExpr() && (cluster->getShardsInfo().size() > 1))
        return writeSplitAsync(block);

    writeAsyncImpl(block);
    ++inserted_blocks;
}


std::string DistributedBlockOutputStream::getCurrentStateDescription()
{
    std::stringstream buffer;
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


void DistributedBlockOutputStream::initWritingJobs(const Block & first_block)
{
    const Settings & settings = context.getSettingsRef();
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = shards_info.size();

    remote_jobs_count = 0;
    local_jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : ext::range(0, shards_info.size()))
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


ThreadPool::Job DistributedBlockOutputStream::runWritingJob(DistributedBlockOutputStream::JobReplica & job, const Block & current_block)
{
    auto thread_group = CurrentThread::getGroup();
    return [this, thread_group, &job, &current_block]()
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
        size_t num_shards = cluster->getShardsInfo().size();
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
        const Settings & settings = context.getSettingsRef();

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
                    auto connections = shard_info.pool->getMany(timeouts, &settings, PoolMode::GET_ONE);
                    if (connections.empty() || connections.front().isNull())
                        throw Exception("Expected exactly one connection for shard " + toString(job.shard_index), ErrorCodes::LOGICAL_ERROR);

                    job.connection_entry = std::move(connections.front());
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

                job.stream = std::make_shared<RemoteBlockOutputStream>(*job.connection_entry, timeouts, query_string, settings, context.getClientInfo());
                job.stream->writePrefix();
            }

            CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};
            job.stream->write(shard_block);
        }
        else // local
        {
            if (!job.stream)
            {
                /// Forward user settings
                job.local_context = std::make_unique<Context>(context);

                InterpreterInsertQuery interp(query_ast, *job.local_context);
                auto block_io = interp.execute();

                job.stream = block_io.out;
                job.stream->writePrefix();
            }

            writeBlockConvert(job.stream, shard_block, shard_info.getLocalNodeCount());
        }

        job.blocks_written += 1;
        job.rows_written += shard_block.rows();
    };
}


void DistributedBlockOutputStream::writeSync(const Block & block)
{
    const Settings & settings = context.getSettingsRef();
    const auto & shards_info = cluster->getShardsInfo();
    size_t num_shards = shards_info.size();

    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        initWritingJobs(block);

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
                pool->scheduleOrThrowOnError(runWritingJob(job, block));
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

    if (insert_sync && pool)
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


void DistributedBlockOutputStream::writeAsyncImpl(const Block & block, const size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    const auto & settings = context.getSettingsRef();

    if (shard_info.hasInternalReplication())
    {
        if (shard_info.isLocal() && settings.prefer_localhost_replica)
            /// Prefer insert into current instance directly
            writeToLocal(block, shard_info.getLocalNodeCount());
        else
            writeToShard(block, {shard_info.pathForInsert(settings.prefer_localhost_replica)});
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


void DistributedBlockOutputStream::writeToLocal(const Block & block, const size_t repeats)
{
    /// Async insert does not support settings forwarding yet whereas sync one supports
    InterpreterInsertQuery interp(query_ast, context);

    auto block_io = interp.execute();

    block_io.out->writePrefix();
    writeBlockConvert(block_io.out, block, repeats);
    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
    /// tmp directory is used to ensure atomicity of transactions
    /// and keep monitor thread out from reading incomplete data
    std::string first_file_tmp_path{};

    auto reservation = storage.getStoragePolicy()->reserve(block.bytes());
    auto disk = reservation->getDisk()->getPath();
    auto data_path = storage.getRelativeDataPath();

    auto it = dir_names.begin();
    /// on first iteration write block to a temporary directory for subsequent
    /// hardlinking to ensure the inode is not freed until we're done
    {
        const std::string path(disk + data_path + *it);
        Poco::File(path).createDirectory();

        const std::string tmp_path(path + "/tmp/");
        Poco::File(tmp_path).createDirectory();

        const std::string file_name(toString(storage.file_names_increment.get()) + ".bin");

        first_file_tmp_path = tmp_path + file_name;

        /// Write batch to temporary location
        {
            WriteBufferFromFile out{first_file_tmp_path};
            CompressedWriteBuffer compress{out};
            NativeBlockOutputStream stream{compress, ClickHouseRevision::get(), block.cloneEmpty()};

            /// Prepare the header.
            /// We wrap the header into a string for compatibility with older versions:
            /// a shard will able to read the header partly and ignore other parts based on its version.
            WriteBufferFromOwnString header_buf;
            writeVarUInt(ClickHouseRevision::get(), header_buf);
            writeStringBinary(query_string, header_buf);
            context.getSettingsRef().write(header_buf);
            context.getClientInfo().write(header_buf, ClickHouseRevision::get());

            /// Add new fields here, for example:
            /// writeVarUInt(my_new_data, header_buf);

            /// Write the header.
            const StringRef header = header_buf.stringRef();
            writeVarUInt(DBMS_DISTRIBUTED_SIGNATURE_HEADER, out);
            writeStringBinary(header, out);
            writePODBinary(CityHash_v1_0_2::CityHash128(header.data, header.size), out);

            stream.writePrefix();
            stream.write(block);
            stream.writeSuffix();
        }

        // Create hardlink here to reuse increment number
        const std::string block_file_path(path + '/' + file_name);
        createHardLink(first_file_tmp_path, block_file_path);
    }
    ++it;

    /// Make hardlinks
    for (; it != dir_names.end(); ++it)
    {
        const std::string path(disk + data_path + *it);
        Poco::File(path).createDirectory();
        const std::string block_file_path(path + '/' + toString(storage.file_names_increment.get()) + ".bin");

        createHardLink(first_file_tmp_path, block_file_path);
    }

    /// remove the temporary file, enabling the OS to reclaim inode after all threads
    /// have removed their corresponding files
    Poco::File(first_file_tmp_path).remove();

    /// Notify
    auto sleep_ms = context.getSettingsRef().distributed_directory_monitor_sleep_time_ms;
    for (const auto & dir_name : dir_names)
    {
        auto & directory_monitor = storage.requireDirectoryMonitor(disk, dir_name);
        directory_monitor.scheduleAfter(sleep_ms.totalMilliseconds());
    }
}


}
