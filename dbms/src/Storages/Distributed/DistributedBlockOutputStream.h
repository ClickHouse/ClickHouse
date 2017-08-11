#pragma once

#include <Parsers/formatAST.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Core/Block.h>
#include <common/ThreadPool.h>
#include <atomic>
#include <memory>
#include <chrono>
#include <experimental/optional>
#include <Interpreters/Cluster.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

class StorageDistributed;

/** If insert_sync_ is true, the write is synchronous. Uses insert_timeout_ if it is not zero.
 *  Otherwise, the write is asynchronous - the data is first written to the local filesystem, and then sent to the remote servers.
 *  If the Distributed table uses more than one shard, then in order to support the write,
 *  when creating the table, an additional parameter must be specified for ENGINE - the sharding key.
 *  Sharding key is an arbitrary expression from the columns. For example, rand() or UserID.
 *  When writing, the data block is splitted by the remainder of the division of the sharding key by the total weight of the shards,
 *  and the resulting blocks are written in a compressed Native format in separate directories for sending.
 *  For each destination address (each directory with data to send), a separate thread is created in StorageDistributed,
 *  which monitors the directory and sends data. */
class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
    DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_, bool insert_sync_, UInt64 insert_timeout_);

    void write(const Block & block) override;

    void writePrefix() override;

private:
    void writeAsync(const Block & block);

    /// Performs synchronous insertion to remote nodes. If timeout_exceeded flag was set, throws.
    void writeSync(const Block & block);

    void calculateJobsCount();

    struct WritingJobContext
    {
        /// Remote job per replica.
        std::vector<bool> done_remote_jobs;
        /// Local job per shard.
        std::vector<bool> done_local_jobs;
        std::atomic<unsigned> finished_jobs_count;
        std::mutex mutex;
        std::condition_variable cond_var;
    };

    ThreadPool::Job createWritingJob(WritingJobContext & context, const Block & block,
                                     const Cluster::Address & address, size_t shard_id, size_t job_id);

    void createWritingJobs(WritingJobContext & context, const Blocks & blocks);

    void waitForUnfinishedJobs(WritingJobContext & context);

    /// Returns the number of blocks was written for each cluster node. Uses during exception handling.
    std::string getCurrentStateDescription(const WritingJobContext & context);

    IColumn::Selector createSelector(Block block);

    /// Split block between shards.
    Blocks splitBlock(const Block & block);

    void writeSplitAsync(const Block & block);

    void writeAsyncImpl(const Block & block, const size_t shard_id = 0);

    /// Increments finished_writings_count after each repeat.
    void writeToLocal(const Block & block, const size_t repeats);

    void writeToShard(const Block & block, const std::vector<std::string> & dir_names);

    /// Performs synchronous insertion to remote node.
    void writeToShardSync(const Block & block, const std::string & connection_pool_name);

private:
    StorageDistributed & storage;
    ASTPtr query_ast;
    ClusterPtr cluster;
    bool insert_sync;
    UInt64 insert_timeout;
    size_t blocks_inserted = 0;
    std::chrono::steady_clock::time_point deadline;
    size_t remote_jobs_count;
    size_t local_jobs_count;
    std::experimental::optional<ThreadPool> pool;
};

}
