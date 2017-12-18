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


DistributedBlockOutputStream::DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast,
                                                           const ClusterPtr & cluster_, bool insert_sync_, UInt64 insert_timeout_)
    : storage(storage), query_ast(query_ast), cluster(cluster_), insert_sync(insert_sync_), insert_timeout(insert_timeout_)
{
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


ThreadPool::Job DistributedBlockOutputStream::createWritingJob(
    WritingJobContext & context, const Block & block, const Cluster::Address & address, size_t shard_id, size_t job_id)
{
    auto memory_tracker = current_memory_tracker;
    return [this, memory_tracker, & context, & block, & address, shard_id, job_id]()
    {
        SCOPE_EXIT({
            std::lock_guard<std::mutex> lock(context.mutex);
            ++context.finished_jobs_count;
            context.cond_var.notify_one();
        });

        if (!current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            setThreadName("DistrOutStrProc");
        }

        const auto & shard_info = cluster->getShardsInfo()[shard_id];
        if (address.is_local)
        {
            writeToLocal(block, shard_info.getLocalNodeCount());
            context.done_local_jobs[job_id] = true;
        }
        else
        {
            writeToShardSync(block, shard_info.hasInternalReplication()
                                    ? shard_info.dir_name_for_internal_replication
                                    : address.toStringFull());
            context.done_remote_jobs[job_id] = true;
        }
    };
}


std::string DistributedBlockOutputStream::getCurrentStateDescription(const WritingJobContext & context)
{
    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();
    String description;
    WriteBufferFromString buffer(description);

    buffer << "Insertion status:\n";

    auto writeDescription = [&buffer](const Cluster::Address & address, size_t shard_id, size_t blocks_wrote)
    {
        buffer << "Wrote " << blocks_wrote << " blocks on shard " << shard_id << " replica ";
        buffer << address.toString() << '\n';
    };

    const auto addresses_with_failovers = cluster->getShardsAddresses();

    size_t remote_job_id = 0;
    size_t local_job_id = 0;
    for (size_t shard_id : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_id];
        /// If hasInternalReplication, than prefer local replica
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal())
        {
            for (const auto & address : addresses_with_failovers[shard_id])
                if (!address.is_local)
                {
                    writeDescription(address, shard_id, blocks_inserted + (context.done_remote_jobs[remote_job_id] ? 1 : 0));
                    ++remote_job_id;
                    if (shard_info.hasInternalReplication())
                        break;
                }
        }

        if (shard_info.isLocal())
        {
            const auto & address = shard_info.local_addresses.front();
            writeDescription(address, shard_id, blocks_inserted + (context.done_local_jobs[local_job_id] ? 1 : 0));
            ++local_job_id;
        }
    }

    return description;
}


void DistributedBlockOutputStream::createWritingJobs(WritingJobContext & context, const Blocks & blocks)
{
    const auto & addresses_with_failovers = cluster->getShardsAddresses();
    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();

    size_t remote_job_id = 0;
    size_t local_job_id = 0;
    for (size_t shard_id : ext::range(0, blocks.size()))
    {
        const auto & shard_info = shards_info[shard_id];
        /// If hasInternalReplication, than prefer local replica
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal())
        {
            for (const auto & address : addresses_with_failovers[shard_id])
                if (!address.is_local)
                {
                    pool->schedule(createWritingJob(context, blocks[shard_id], address, shard_id, remote_job_id));
                    ++remote_job_id;
                    if (shard_info.hasInternalReplication())
                        break;
                }
        }

        if (shards_info[shard_id].isLocal())
        {
            const auto & address = shards_info[shard_id].local_addresses.front();
            pool->schedule(createWritingJob(context, blocks[shard_id], address, shard_id, local_job_id));
            ++local_job_id;
        }
    }
}


void DistributedBlockOutputStream::calculateJobsCount()
{
    remote_jobs_count = 0;
    local_jobs_count = 0;

    const auto & addresses_with_failovers = cluster->getShardsAddresses();

    const auto & shards_info = cluster->getShardsInfo();
    for (size_t shard_id : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_id];
        /// If hasInternalReplication, than prefer local replica
        if (!shard_info.hasInternalReplication() || !shard_info.isLocal())
        {
            for (const auto & address : addresses_with_failovers[shard_id])
                if (!address.is_local)
                {
                    ++remote_jobs_count;
                    if (shard_info.hasInternalReplication())
                        break;
                }
        }

        local_jobs_count += shard_info.isLocal() ? 1 : 0;
    }
}


void DistributedBlockOutputStream::waitForUnfinishedJobs(WritingJobContext & context)
{
    size_t jobs_count = remote_jobs_count + local_jobs_count;
    auto cond = [& context, jobs_count] { return context.finished_jobs_count == jobs_count; };

    if (insert_timeout)
    {
        bool were_jobs_finished;
        {
            std::unique_lock<std::mutex> lock(context.mutex);
            were_jobs_finished = context.cond_var.wait_until(lock, deadline, cond);
        }
        if (!were_jobs_finished)
        {
            pool->wait();
            ProfileEvents::increment(ProfileEvents::DistributedSyncInsertionTimeoutExceeded);
            throw Exception("Timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(context.mutex);
        context.cond_var.wait(lock, cond);
    }
    pool->wait();
}


void DistributedBlockOutputStream::writeSync(const Block & block)
{
    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        calculateJobsCount();
        pool.emplace(remote_jobs_count + local_jobs_count);
    }

    WritingJobContext context;
    context.done_remote_jobs.assign(remote_jobs_count, false);
    context.done_local_jobs.assign(local_jobs_count, false);
    context.finished_jobs_count = 0;

    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();
    Blocks blocks = shards_info.size() > 1 ? splitBlock(block) : Blocks({block});
    createWritingJobs(context, blocks);

    try
    {
        waitForUnfinishedJobs(context);
    }
    catch (Exception & exception)
    {
        exception.addMessage(getCurrentStateDescription(context));
        throw;
    }

    ++blocks_inserted;
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
    InterpreterInsertQuery interp{query_ast, storage.context};

    auto block_io = interp.execute();
    block_io.out->writePrefix();

    for (size_t i = 0; i < repeats; ++i)
        block_io.out->write(block);

    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShardSync(const Block & block, const std::string & connection_pool_name)
{
    auto pool = storage.requireConnectionPool(connection_pool_name);
    auto connection = pool->get();

    const auto & query_string = queryToString(query_ast);
    RemoteBlockOutputStream remote{*connection, query_string};

    CurrentMetrics::Increment metric_increment{CurrentMetrics::DistributedSend};

    remote.writePrefix();
    remote.write(block);
    remote.writeSuffix();
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
            NativeBlockOutputStream stream{compress, ClickHouseRevision::get()};

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
