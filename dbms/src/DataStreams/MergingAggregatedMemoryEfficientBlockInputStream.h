#pragma once

#include <Interpreters/Aggregator.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <common/ThreadPool.h>
#include <condition_variable>


class MemoryTracker;

namespace DB
{


/** Pre-aggregates block streams, holding in RAM only one or more (up to merging_threads) blocks from each source.
  * This saves RAM in case of using two-level aggregation, where in each source there will be up to 256 blocks with parts of the result.
  *
  * Aggregate functions in blocks should not be finalized so that their states can be combined.
  *
  * Used to solve two tasks:
  *
  * 1. External aggregation with data flush to disk.
  * Partially aggregated data (previously divided into 256 buckets) is flushed to some number of files on the disk.
  * We need to read them and merge them by buckets - keeping only a few buckets from each file in RAM simultaneously.
  *
  * 2. Merge aggregation results for distributed query processing.
  * Partially aggregated data arrives from different servers, which can be splitted down or not, into 256 buckets,
  *  and these buckets are passed to us by the network from each server in sequence, one by one.
  * You should also read and merge by the buckets.
  *
  * The essence of the work:
  *
  * There are a number of sources. They give out blocks with partially aggregated data.
  * Each source can return one of the following block sequences:
  * 1. "unsplitted" block with bucket_num = -1;
  * 2. "splitted" (two_level) blocks with bucket_num from 0 to 255;
  * In both cases, there may also be a block of "overflows" with bucket_num = -1 and is_overflows = true;
  *
  * We start from the convention that splitted blocks are always passed in the order of bucket_num.
  * That is, if a < b, then the bucket_num = a block goes before bucket_num = b.
  * This is needed for a memory-efficient merge
  * - so that you do not need to read the blocks up front, but go all the way up by bucket_num.
  *
  * In this case, not all bucket_num from the range of 0..255 can be present.
  * The overflow block can be presented in any order relative to other blocks (but it can be only one).
  *
  * It is necessary to combine these sequences of blocks and return the result as a sequence with the same properties.
  * That is, at the output, if there are "splitted" blocks in the sequence, then they should go in the order of bucket_num.
  *
  * The merge can be performed using several (merging_threads) threads.
  * For this, receiving of a set of blocks for the next bucket_num should be done sequentially,
  *  and then, when we have several received sets, they can be merged in parallel.
  *
  * When you receive next blocks from different sources,
  *  data from sources can also be read in several threads (reading_threads)
  *  for optimal performance in the presence of a fast network or disks (from where these blocks are read).
  */
class MergingAggregatedMemoryEfficientBlockInputStream final : public IProfilingBlockInputStream
{
public:
    MergingAggregatedMemoryEfficientBlockInputStream(
        BlockInputStreams inputs_, const Aggregator::Params & params, bool final_,
        size_t reading_threads_, size_t merging_threads_);

    ~MergingAggregatedMemoryEfficientBlockInputStream() override;

    String getName() const override { return "MergingAggregatedMemoryEfficient"; }

    /// Sends the request (initiates calculations) earlier than `read`.
    void readPrefix() override;

    /// Called either after everything is read, or after cancel.
    void readSuffix() override;

    /** Different from the default implementation by trying to stop all sources,
      *  skipping failed by execution.
      */
    void cancel(bool kill) override;

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    static constexpr int NUM_BUCKETS = 256;

    Aggregator aggregator;
    bool final;
    size_t reading_threads;
    size_t merging_threads;

    bool started = false;
    bool all_read = false;
    std::atomic<bool> has_two_level {false};
    std::atomic<bool> has_overflows {false};
    int current_bucket_num = -1;

    Logger * log = &Logger::get("MergingAggregatedMemoryEfficientBlockInputStream");


    struct Input
    {
        BlockInputStreamPtr stream;
        Block block;
        Block overflow_block;
        std::vector<Block> splitted_blocks;
        bool is_exhausted = false;

        Input(BlockInputStreamPtr & stream_) : stream(stream_) {}
    };

    std::vector<Input> inputs;

    using BlocksToMerge = std::unique_ptr<BlocksList>;

    void start();

    /// Get blocks that you can merge. This allows you to merge them in parallel in separate threads.
    BlocksToMerge getNextBlocksToMerge();

    std::unique_ptr<ThreadPool> reading_pool;

    /// For a parallel merge.

    struct ParallelMergeData
    {
        ThreadPool pool;

        /// Now one of the merging threads receives next blocks for the merge. This operation must be done sequentially.
        std::mutex get_next_blocks_mutex;

        std::atomic<bool> exhausted {false};    /// No more source data.
        std::atomic<bool> finish {false};        /// Need to terminate early.

        std::exception_ptr exception;
        /// It is necessary to give out blocks in the order of the key (bucket_num).
        /// If the value is an empty block, you need to wait for its merge.
        /// (This means the promise that there will be data here, which is important because the data should be given out
        /// in the order of the key - bucket_num)
        std::map<int, Block> merged_blocks;
        std::mutex merged_blocks_mutex;
        /// An event that is used by merging threads to tell the main thread that the new block is ready.
        std::condition_variable merged_blocks_changed;
        /// An event by which the main thread is telling merging threads that it is possible to process the next group of blocks.
        std::condition_variable have_space;

        explicit ParallelMergeData(size_t max_threads) : pool(max_threads) {}
    };

    std::unique_ptr<ParallelMergeData> parallel_merge_data;

    void mergeThread(MemoryTracker * memory_tracker);

    void finalize();
};

}
