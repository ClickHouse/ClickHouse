#pragma once

#include <Parsers/formatAST.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Core/Block.h>
#include <Common/PODArray.h>
#include <Common/Throttler.h>
#include <Common/ThreadPool.h>
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

class Context;
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
    DistributedBlockOutputStream(
        const Context & context_,
        StorageDistributed & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ASTPtr & query_ast_,
        const ClusterPtr & cluster_,
        bool insert_sync_,
        UInt64 insert_timeout_);

    Block getHeader() const override;
    void write(const Block & block) override;
    void writePrefix() override;

    void writeSuffix() override;

private:
    IColumn::Selector createSelector(const Block & source_block) const;

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

    void initWritingJobs(const Block & first_block);

    struct JobReplica;
    ThreadPool::Job runWritingJob(JobReplica & job, const Block & current_block);

    void waitForJobs();

    /// Returns the number of blocks was written for each cluster node. Uses during exception handling.
    std::string getCurrentStateDescription();

private:
    const Context & context;
    StorageDistributed & storage;
    StorageMetadataPtr metadata_snapshot;
    ASTPtr query_ast;
    String query_string;
    ClusterPtr cluster;
    size_t inserted_blocks = 0;
    size_t inserted_rows = 0;

    bool insert_sync;

    /// Sync-related stuff
    UInt64 insert_timeout; // in seconds
    Stopwatch watch;
    Stopwatch watch_current_block;
    std::optional<ThreadPool> pool;
    ThrottlerPtr throttler;

    struct JobReplica
    {
        JobReplica() = default;
        JobReplica(size_t shard_index_, size_t replica_index_, bool is_local_job_, const Block & sample_block)
            : shard_index(shard_index_), replica_index(replica_index_), is_local_job(is_local_job_), current_shard_block(sample_block.cloneEmpty()) {}

        size_t shard_index = 0;
        size_t replica_index = 0;
        bool is_local_job = false;

        Block current_shard_block;

        ConnectionPool::Entry connection_entry;
        std::unique_ptr<Context> local_context;
        BlockOutputStreamPtr stream;

        UInt64 blocks_written = 0;
        UInt64 rows_written = 0;

        UInt64 blocks_started = 0;
        UInt64 elapsed_time_ms = 0;
        UInt64 max_elapsed_time_for_block_ms = 0;
    };

    struct JobShard
    {
        std::list<JobReplica> replicas_jobs;
        IColumn::Permutation shard_current_block_permutation;
    };

    std::vector<JobShard> per_shard_jobs;

    size_t remote_jobs_count = 0;
    size_t local_jobs_count = 0;

    std::atomic<unsigned> finished_jobs_count{0};

    Poco::Logger * log;
};

}
