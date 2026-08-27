#pragma once

#include <Core/SortDescription.h>
#include <Common/HashTable/HashSet.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{

/** Pre-aggregates data from ports, holding in RAM only one or more (up to merging_threads) blocks from each source.
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
  * Partially aggregated data arrives from different servers, which can be split down or not, into 256 buckets,
  *  and these buckets are passed to us by the network from each server in sequence, one by one.
  * You should also read and merge by the buckets.
  *
  * The essence of the work:
  *
  * There are a number of sources. They give out blocks with partially aggregated data.
  * Each source can return one of the following block sequences:
  * 1. "unsplitted" block with bucket_num = -1;
  * 2. "split" (two_level) blocks with bucket_num from 0 to 255;
  * In both cases, there may also be a block of "overflows" with bucket_num = -1 and is_overflows = true;
  *
  * We start from the convention that split blocks are always passed in the order of bucket_num.
  * That is, if a < b, then the bucket_num = a block goes before bucket_num = b.
  * This is needed for a memory-efficient merge
  * - so that you do not need to read the blocks up front, but go all the way up by bucket_num.
  *
  * In this case, not all bucket_num from the range of 0..255 can be present.
  * The overflow block can be presented in any order relative to other blocks (but it can be only one).
  *
  * It is necessary to combine these sequences of blocks and return the result as a sequence with the same properties.
  * That is, at the output, if there are "split" blocks in the sequence, then they should go in the order of bucket_num.
  *
  * The merge can be performed using several (merging_threads) threads.
  * For this, receiving of a set of blocks for the next bucket_num should be done sequentially,
  *  and then, when we have several received sets, they can be merged in parallel.
  *
  * When you receive next blocks from different sources,
  *  data from sources can also be read in several threads (reading_threads)
  *  for optimal performance in the presence of a fast network or disks (from where these blocks are read).
  */

/// Has several inputs and single output.
/// Read from inputs chunks with partially aggregated data, group them by bucket number
///  and write data from single bucket as single chunk.
class GroupingAggregatedTransform : public IProcessor
{
public:
    GroupingAggregatedTransform(const Block & header_, size_t num_inputs_, AggregatingTransformParamsPtr params_);
    String getName() const override { return "GroupingAggregatedTransform"; }

    /// Special setting: in case if single source can return several chunks with same bucket.
    void allowSeveralChunksForSingleBucketPerSource() { expect_several_chunks_for_single_bucket_per_source = true; }

protected:
    Status prepare(const PortNumbers & updated_input_ports, const PortNumbers &) override;
    void work() override;

private:
    size_t num_inputs;
    AggregatingTransformParamsPtr params;

    std::vector<Int32> last_bucket_number; /// Last bucket read from each input.
    std::map<Int32, Chunks> chunks_map; /// bucket -> chunks
    Chunks overflow_chunks;
    Chunks single_level_chunks;
    Int32 current_bucket = 0; /// Currently processing bucket.
    Int32 next_bucket_to_push = 0; /// Always <= current_bucket.
    bool has_two_level = false;

    bool all_inputs_finished = false;
    bool initialized_index_to_input = false;
    std::vector<InputPorts::iterator> index_to_input;
    HashSet<uint64_t> wait_input_ports_numbers;

    /// If we aggregate partitioned data several chunks might be produced for the same bucket: one for each partition.
    bool expect_several_chunks_for_single_bucket_per_source = true;

    /// Add chunk read from input to chunks_map, overflow_chunks or single_level_chunks according to it's chunk info.
    void addChunk(Chunk chunk, size_t input);
    /// Push chunks if all inputs has single level.
    bool tryPushSingleLevelData();
    /// Push chunks from ready bucket if has one.
    bool tryPushTwoLevelData();
    /// Push overflow chunks if has any.
    bool tryPushOverflowData();
    /// Push chunks from bucket to output port.
    void pushData(Chunks chunks, Int32 bucket, bool is_overflows);
};

/// Merge aggregated data from single bucket.
class MergingAggregatedBucketTransform : public ISimpleTransform
{
public:
    explicit MergingAggregatedBucketTransform(
        AggregatingTransformParamsPtr params, const SortDescription & required_sort_description_ = {});
    String getName() const override { return "MergingAggregatedBucketTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    AggregatingTransformParamsPtr params;
    const SortDescription required_sort_description;
};

/// Has several inputs and single output.
/// Read from inputs merged bucket with aggregated data, sort them by bucket number and write to output.
/// Presumption: inputs return chunks with increasing bucket number, there is at most one chunk per bucket.
class SortingAggregatedTransform : public IProcessor
{
public:
    SortingAggregatedTransform(size_t num_inputs, AggregatingTransformParamsPtr params);
    String getName() const override { return "SortingAggregatedTransform"; }
    Status prepare() override;

private:
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    std::vector<Int32> last_bucket_number;
    std::vector<bool> is_input_finished;
    std::map<Int32, Chunk> chunks;
    Chunk overflow_chunk;

    bool tryPushChunk();
    void addChunk(Chunk chunk, size_t from_input);
};

struct ChunksToMerge : public ChunkInfoCloneable<ChunksToMerge>
{
    std::shared_ptr<Chunks> chunks;
    Int32 bucket_num = -1;
    bool is_overflows = false;
    UInt64 chunk_num = 0; // chunk number in order of generation, used during memory bound merging to restore chunks order
};

class Pipe;

/// Adds processors to pipe which performs memory efficient merging of partially aggregated data from several sources.
void addMergingAggregatedMemoryEfficientTransform(
    Pipe & pipe,
    AggregatingTransformParamsPtr params,
    size_t num_merging_processors);

}

