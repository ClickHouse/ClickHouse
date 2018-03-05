#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>

#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/RemoteBlockOutputStream.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/createBlockSelector.h>

#include <DataTypes/DataTypesNumber.h>
#include <Common/setThreadName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/CurrentMetrics.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <Common/escapeForFileName.h>
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
    extern const int TIMEOUT_EXCEEDED;
}


DistributedBlockOutputStream::DistributedBlockOutputStream(
    StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_,
    const Settings & settings_, bool insert_sync_, UInt64 insert_timeout_)
    : storage(storage), query_ast(query_ast), cluster(cluster_), settings(settings_), insert_sync(insert_sync_), insert_timeout(insert_timeout_)
{
}


Block DistributedBlockOutputStream::getHeader() const
{
    return storage.getSampleBlock();
}


void DistributedBlockOutputStream::writePrefix()
{
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(insert_timeout);
}


void DistributedBlockOutputStream::write(const Block & block)
{
    if (insert_sync)
        writeSync(block);
    else
        writeAsync(block);
}


void DistributedBlockOutputStream::writeAsync(const Block & block)
{
    if (storage.getShardingKeyExpr() && (cluster->getShardsInfo().size() > 1))
        return writeSplitAsync(block);

    writeAsyncImpl(block);
    ++blocks_inserted;
}


std::string DistributedBlockOutputStream::getCurrentStateDescription()
{
    std::stringstream buffer;
    const auto & addresses = cluster->getShardsAddresses();

    buffer << "Insertion status:\n";
    for (auto & shard_jobs : per_shard_jobs)
        for (JobInfo & job : shard_jobs)
        {
            buffer << "Wrote " << job.blocks_written << " blocks and " << job.rows_written << " rows"
                   << " on shard " << job.shard_index << " replica " << job.replica_index
                   << ", " << addresses[job.shard_index][job.replica_index].readableString() << "\n";
        }

    return buffer.str();
}


void DistributedBlockOutputStream::initWritingJobs()
{
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const auto & shards_info = cluster->getShardsInfo();

    remote_jobs_count = 0;
    local_jobs_count = 0;
    per_shard_jobs.resize(shards_info.size());

    for (size_t shard_index : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_index];
        auto & shard_jobs = per_shard_jobs[shard_index];

        /// If hasInternalReplication, than prefer local replica
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal())
        {
            const auto & replicas = addresses_with_failovers[shard_index];

            for (size_t replica_index : ext::range(0, replicas.size()))
            {
                if (!replicas[replica_index].is_local)
                {
                    shard_jobs.emplace_back(shard_index, replica_index, false);
                    ++remote_jobs_count;

                    if (shard_info.hasInternalReplication())
                        break;
                }
            }
        }

        if (shard_info.isLocal())
        {
            shard_jobs.emplace_back(shard_index, 0, true);
            ++local_jobs_count;
        }
    }
}


void DistributedBlockOutputStream::waitForJobs()
{
    size_t jobs_count = remote_jobs_count + local_jobs_count;
    auto cond = [this, jobs_count] { return finished_jobs_count >= jobs_count; };

    if (insert_timeout)
    {
        bool were_jobs_finished;
        {
            std::unique_lock<std::mutex> lock(mutex);
            were_jobs_finished = cond_var.wait_until(lock, deadline, cond);
        }

        pool->wait();

        if (!were_jobs_finished)
        {
            ProfileEvents::increment(ProfileEvents::DistributedSyncInsertionTimeoutExceeded);
            throw Exception("Synchronous distributed insert timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(mutex);
        cond_var.wait(lock, cond);
        pool->wait();
    }
}


ThreadPool::Job DistributedBlockOutputStream::runWritingJob(DistributedBlockOutputStream::JobInfo & job)
{
    auto memory_tracker = current_memory_tracker;
    return [this, memory_tracker, &job]()
    {
        SCOPE_EXIT({
            std::lock_guard<std::mutex> lock(mutex);
            ++finished_jobs_count;
            cond_var.notify_one();
        });

        if (!current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            setThreadName("DistrOutStrProc");
        }

        const auto & shard_info = cluster->getShardsInfo()[job.shard_index];
        const auto & addresses = cluster->getShardsAddresses();
        Block & block = current_blocks.at(job.shard_index);

        if (!job.is_local_job)
        {
            if (!job.stream)
            {
                if (shard_info.hasInternalReplication())
                {
                    /// Skip replica_index in case of internal replication
                    if (per_shard_jobs[job.shard_index].size() != 1)
                        throw Exception("There are several writing job for an automatically replicated shard", ErrorCodes::LOGICAL_ERROR);

                    /// TODO: it make sense to rewrite skip_unavailable_shards and max_parallel_replicas here
                    auto connections = shard_info.pool->getMany(&settings, PoolMode::GET_ONE);
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

                    job.connection_entry = connection_pool->get(&settings);
                    if (job.connection_entry.isNull())
                        throw Exception("Got empty connection for replica" + replica.readableString(), ErrorCodes::LOGICAL_ERROR);
                }

                if (throttler)
                    job.connection_entry->setThrottler(throttler);

                job.stream = std::make_shared<RemoteBlockOutputStream>(*job.connection_entry, query_string, &settings);
                job.stream->writePrefix();
            }

            CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};
            job.stream->write(block);
        }
        else
        {
            if (!job.stream)
            {
                /// Forward user settings
                job.local_context = std::make_unique<Context>(storage.context);
                job.local_context->setSettings(settings);

                InterpreterInsertQuery interp(query_ast, *job.local_context);
                job.stream = interp.execute().out;
                job.stream->writePrefix();
            }

            size_t num_repetitions = shard_info.getLocalNodeCount();
            for (size_t i = 0; i < num_repetitions; ++i)
                job.stream->write(block);
        }

        ++job.blocks_written;
        job.rows_written += block.rows();
    };
}


void DistributedBlockOutputStream::writeSync(const Block & block)
{
    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        initWritingJobs();
        pool.emplace(remote_jobs_count + local_jobs_count);
        query_string = queryToString(query_ast);

        if (!throttler && (settings.limits.max_network_bandwidth || settings.limits.max_network_bytes))
        {
            throttler = std::make_shared<Throttler>(settings.limits.max_network_bandwidth, settings.limits.max_network_bytes,
                                                    "Network bandwidth limit for a query exceeded.");
        }
    }

    const auto & shards_info = cluster->getShardsInfo();
    current_blocks = shards_info.size() > 1 ? splitBlock(block) : Blocks({block});

    /// Run jobs in parallel for each block and wait them
    finished_jobs_count = 0;
    for (size_t shard_index : ext::range(0, current_blocks.size()))
        for (JobInfo & job : per_shard_jobs.at(shard_index))
            pool->schedule(runWritingJob(job));

    try
    {
        waitForJobs();
    }
    catch (Exception & exception)
    {
        exception.addMessage(getCurrentStateDescription());
        throw;
    }

    ++blocks_inserted;
}


void DistributedBlockOutputStream::writeSuffix()
{
    if (insert_sync && pool)
    {
        finished_jobs_count = 0;
        for (auto & shard_jobs : per_shard_jobs)
            for (JobInfo & job : shard_jobs)
            {
                if (job.stream)
                    pool->schedule([&job] () { job.stream->writeSuffix(); });
            }

        pool->wait();

        LOG_DEBUG(&Logger::get("DistributedBlockOutputStream"), getCurrentStateDescription());
    }
}


IColumn::Selector DistributedBlockOutputStream::createSelector(Block block)
{
    storage.getShardingKeyExpr()->execute(block);
    const auto & key_column = block.getByName(storage.getShardingKeyColumnName());
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}


Blocks DistributedBlockOutputStream::splitBlock(const Block & block)
{
    const auto num_cols = block.columns();
    /// cache column pointers for later reuse
    std::vector<const IColumn *> columns(num_cols);
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i] = block.safeGetByPosition(i).column.get();

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

    ++blocks_inserted;
}


void DistributedBlockOutputStream::writeAsyncImpl(const Block & block, const size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];

    if (shard_info.hasInternalReplication())
    {
        if (shard_info.getLocalNodeCount() > 0)
        {
            /// Prefer insert into current instance directly
            writeToLocal(block, shard_info.getLocalNodeCount());
        }
        else
        {
            if (shard_info.dir_name_for_internal_replication.empty())
                throw Exception("Directory name for async inserts is empty, table " + storage.getTableName(), ErrorCodes::LOGICAL_ERROR);

            writeToShard(block, {shard_info.dir_name_for_internal_replication});
        }
    }
    else
    {
        if (shard_info.getLocalNodeCount() > 0)
            writeToLocal(block, shard_info.getLocalNodeCount());

        std::vector<std::string> dir_names;
        for (const auto & address : cluster->getShardsAddresses()[shard_id])
            if (!address.is_local)
                dir_names.push_back(address.toStringFull());

        if (!dir_names.empty())
            writeToShard(block, dir_names);
    }
}


void DistributedBlockOutputStream::writeToLocal(const Block & block, const size_t repeats)
{
    /// Async insert does not support settings forwarding yet whereas sync one supports
    InterpreterInsertQuery interp(query_ast, storage.context);

    auto block_io = interp.execute();
    block_io.out->writePrefix();

    for (size_t i = 0; i < repeats; ++i)
        block_io.out->write(block);

    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
    /** tmp directory is used to ensure atomicity of transactions
      *  and keep monitor thread out from reading incomplete data
      */
    std::string first_file_tmp_path{};

    auto first = true;
    const auto & query_string = queryToString(query_ast);

    /// write first file, hardlink the others
    for (const auto & dir_name : dir_names)
    {
        const auto & path = storage.getPath() + dir_name + '/';

        /// ensure shard subdirectory creation and notify storage
        if (Poco::File(path).createDirectory())
            storage.requireDirectoryMonitor(dir_name);

        const auto & file_name = toString(storage.file_names_increment.get()) + ".bin";
        const auto & block_file_path = path + file_name;

        /** on first iteration write block to a temporary directory for subsequent hardlinking to ensure
            *  the inode is not freed until we're done */
        if (first)
        {
            first = false;

            const auto & tmp_path = path + "tmp/";
            Poco::File(tmp_path).createDirectory();
            const auto & block_file_tmp_path = tmp_path + file_name;

            first_file_tmp_path = block_file_tmp_path;

            WriteBufferFromFile out{block_file_tmp_path};
            CompressedWriteBuffer compress{out};
            NativeBlockOutputStream stream{compress, ClickHouseRevision::get(), block.cloneEmpty()};

            writeStringBinary(query_string, out);

            stream.writePrefix();
            stream.write(block);
            stream.writeSuffix();
        }

        if (link(first_file_tmp_path.data(), block_file_path.data()))
            throwFromErrno("Could not link " + block_file_path + " to " + first_file_tmp_path);
    }

    /** remove the temporary file, enabling the OS to reclaim inode after all threads
        *  have removed their corresponding files */
    Poco::File(first_file_tmp_path).remove();
}


}
