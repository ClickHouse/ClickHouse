#pragma once

#include <Parsers/formatAST.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Core/Block.h>
#include <Common/Throttler.h>
#include <common/ThreadPool.h>
#include <atomic>
#include <memory>
#include <chrono>
#include <optional>
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
    DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_,
                                 const Settings & settings_, bool insert_sync_, UInt64 insert_timeout_);

    Block getHeader() const override;
    void write(const Block & block) override;
    void writePrefix() override;

    void writeSuffix() override;

private:

    IColumn::Selector createSelector(Block block);


    void writeAsync(const Block & block);

    /// Split block between shards.
    Blocks splitBlock(const Block & block);

    void writeSplitAsync(const Block & block);

    void writeAsyncImpl(const Block & block, const size_t shard_id = 0);

    /// Increments finished_writings_count after each repeat.
    void writeToLocal(const Block & block, const size_t repeats);

    void writeToShard(const Block & block, const std::vector<std::string> & dir_names);


    /// Performs synchronous insertion to remote nodes. If timeout_exceeded flag was set, throws.
    void writeSync(const Block & block);

    void initWritingJobs();

    struct JobInfo;
    ThreadPool::Job runWritingJob(JobInfo & job);

    void waitForJobs();

    /// Returns the number of blocks was written for each cluster node. Uses during exception handling.
    std::string getCurrentStateDescription();

private:
    StorageDistributed & storage;
    ASTPtr query_ast;
    ClusterPtr cluster;
    const Settings & settings;
    size_t blocks_inserted = 0;

    bool insert_sync;

    /// Sync-related stuff
    UInt64 insert_timeout;
    std::chrono::steady_clock::time_point deadline;
    std::optional<ThreadPool> pool;
    ThrottlerPtr throttler;
    String query_string;

    struct JobInfo
    {
        JobInfo() = default;
        JobInfo(size_t shard_index, size_t replica_index, bool is_local_job)
            : shard_index(shard_index), replica_index(replica_index), is_local_job(is_local_job) {}

        size_t shard_index = 0;
        size_t replica_index = 0;
        bool is_local_job = false;

        ConnectionPool::Entry connection_entry;
        std::unique_ptr<Context> local_context;
        BlockOutputStreamPtr stream;

        UInt64 blocks_written = 0;
        UInt64 rows_written = 0;
    };

    std::vector<std::list<JobInfo>> per_shard_jobs;
    Blocks current_blocks;

    size_t remote_jobs_count = 0;
    size_t local_jobs_count = 0;

    std::atomic<unsigned> finished_jobs_count{0};
    std::mutex mutex;
    std::condition_variable cond_var;
};

}
