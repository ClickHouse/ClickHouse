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
        return writeSplit(block);

    writeImpl(block);
    ++blocks_inserted;
}

ThreadPool::Job DistributedBlockOutputStream::createWritingJob(
    std::vector<bool> & done_jobs, std::atomic<unsigned> & finished_jobs_count, std::condition_variable & cond_var,
    const Block & block, size_t job_id, const Cluster::ShardInfo & shard_info, size_t replica_id)
{
    auto memory_tracker = current_memory_tracker;
    return [this, memory_tracker, & done_jobs, & finished_jobs_count, & cond_var, & block, job_id, & shard_info, replica_id]()
    {
        if (!current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            setThreadName("DistrOutStrProc");
        }
        try
        {
            this->writeToShardSync(block, shard_info, replica_id);
            ++finished_jobs_count;
            done_jobs[job_id] = true;
            cond_var.notify_one();
        }
        catch (...)
        {
            ++finished_jobs_count;
            cond_var.notify_one();
            throw;
        }
    };
}

void DistributedBlockOutputStream::writeToLocal(const Blocks & blocks, size_t & finished_writings_count)
{
    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();
    for (size_t shard_id : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_id];
        if (shard_info.getLocalNodeCount() > 0)
            writeToLocal(blocks[shard_id], shard_info.getLocalNodeCount(), finished_writings_count);
    }
}


std::string DistributedBlockOutputStream::getCurrentStateDescription(
    const std::vector<bool> & done_jobs, size_t finished_local_nodes_count)
{
    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();
    String description;
    WriteBufferFromString buffer(description);

    buffer << "Insertion status:\n";

    auto writeDescription = [&buffer](const std::string & address, size_t shard_id, size_t blocks_wrote)
    {
        buffer << "Wrote " << blocks_wrote << " blocks on shard " << shard_id << " replica ";
        buffer << unescapeForFileName(address) << '\n';
    };

    size_t job_id = 0;
    for (size_t shard_id : ext::range(0, shards_info.size()))
    {
        const auto & shard_info = shards_info[shard_id];
        const auto & local_addresses = shard_info.local_addresses;

        for (const auto & address : local_addresses)
        {
            writeDescription(address.toStringFull(), shard_id, blocks_inserted + (finished_local_nodes_count ? 1 : 0));
            if (finished_local_nodes_count)
                --finished_local_nodes_count;
        }

        for (const auto & dir_name : shard_info.dir_names)
            writeDescription(dir_name, shard_id, blocks_inserted + (done_jobs[job_id++] ? 1 : 0));
    }

    return description;
}

void DistributedBlockOutputStream::calculateRemoteJobsCount()
{
    remote_jobs_count = 0;
    const auto & shards_info = cluster->getShardsInfo();
    for (const auto & shard_info : shards_info)
        remote_jobs_count += shard_info.dir_names.size();
}

void DistributedBlockOutputStream::writeSync(const Block & block)
{
    if (!pool)
    {
        /// Deferred initialization. Only for sync insertion.
        calculateRemoteJobsCount();
        pool.emplace(remote_jobs_count);
    }

    std::vector<bool> done_jobs(remote_jobs_count, false);
    std::atomic<unsigned> finished_jobs_count(0);
    std::mutex mutex;
    std::condition_variable cond_var;

    const Cluster::ShardsInfo & shards_info = cluster->getShardsInfo();
    Blocks blocks = shards_info.size() > 1 ? splitBlock(block) : Blocks({block});

    size_t job_id = 0;
    for (size_t shard_id : ext::range(0, blocks.size()))
        for (size_t replica_id: ext::range(0, shards_info[shard_id].dir_names.size()))
            pool->schedule(createWritingJob(done_jobs, finished_jobs_count, cond_var,
                                            blocks[shard_id], job_id++, shards_info[shard_id], replica_id));

    const size_t jobs_count = job_id;
    size_t finished_local_nodes_count;
    const auto time_point = deadline;
    auto timeout = insert_timeout;
    auto & pool = this->pool;
    auto wait = [& mutex, & cond_var, time_point, & finished_jobs_count, jobs_count, & pool, timeout]()
    {
        std::unique_lock<std::mutex> lock(mutex);
        auto cond = [& finished_jobs_count, jobs_count] { return finished_jobs_count == jobs_count; };
        if (timeout)
        {
            if (!cond_var.wait_until(lock, time_point, cond))
            {
                pool->wait();
                ProfileEvents::increment(ProfileEvents::DistributedSyncInsertionTimeoutExceeded);
                throw Exception("Timeout exceeded.", ErrorCodes::TIMEOUT_EXCEEDED);
            }
        }
        else
            cond_var.wait(lock, cond);
        pool->wait();
    };

    std::exception_ptr exception;
    try
    {
        writeToLocal(blocks, finished_local_nodes_count);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    try
    {
        wait();
        if (exception)
            std::rethrow_exception(exception);
    }
    catch (Exception & exception)
    {
        exception.addMessage(getCurrentStateDescription(done_jobs, finished_local_nodes_count));
        throw;
    }

    ++blocks_inserted;
}

IColumn::Selector DistributedBlockOutputStream::createSelector(Block block)
{
    storage.getShardingKeyExpr()->execute(block);
    const auto & key_column = block.getByName(storage.getShardingKeyColumnName());
    size_t num_shards = cluster->getShardsInfo().size();
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, num_shards, slot_to_shard);

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
        Columns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_shards, selector);
        for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
            splitted_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(splitted_columns[shard_idx]);
    }

    return splitted_blocks;
}


void DistributedBlockOutputStream::writeSplit(const Block & block)
{
    Blocks splitted_blocks = splitBlock(block);
    const size_t num_shards = splitted_blocks.size();

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        if (splitted_blocks[shard_idx].rows())
            writeImpl(splitted_blocks[shard_idx], shard_idx);

    ++blocks_inserted;
}


void DistributedBlockOutputStream::writeImpl(const Block & block, const size_t shard_id)
{
    const auto & shard_info = cluster->getShardsInfo()[shard_id];
    size_t finished_writings_count = 0;
    if (shard_info.getLocalNodeCount() > 0)
        writeToLocal(block, shard_info.getLocalNodeCount(), finished_writings_count);

    /// dir_names is empty if shard has only local addresses
    if (!shard_info.dir_names.empty())
        writeToShard(block, shard_info.dir_names);
}


void DistributedBlockOutputStream::writeToLocal(const Block & block, const size_t repeats, size_t & finished_writings_count)
{
    InterpreterInsertQuery interp{query_ast, storage.context};

    auto block_io = interp.execute();
    block_io.out->writePrefix();

    for (size_t i = 0; i < repeats; ++i)
    {
        block_io.out->write(block);
        ++finished_writings_count;
    }

    block_io.out->writeSuffix();
}


void DistributedBlockOutputStream::writeToShardSync(
    const Block & block, const Cluster::ShardInfo & shard_info, size_t replica_id)
{
    const auto & dir_name = shard_info.dir_names[replica_id];
    auto pool = storage.requireConnectionPool(dir_name);
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
