#include <Processors/IProcessor.h>
#include <Interpreters/Aggregator.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/ResizeProcessor.h>


namespace DB
{

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
    Status prepare() override;
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
    bool read_from_all_inputs = false;
    std::vector<bool> read_from_input;

    bool expect_several_chunks_for_single_bucket_per_source = false;

    /// Add chunk read from input to chunks_map, overflow_chunks or single_level_chunks according to it's chunk info.
    void addChunk(Chunk chunk, size_t input);
    /// Read from all inputs first chunk. It is needed to detect if any source has two-level aggregation.
    void readFromAllInputs();
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
    explicit MergingAggregatedBucketTransform(AggregatingTransformParamsPtr params);
    String getName() const override { return "MergingAggregatedBucketTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    AggregatingTransformParamsPtr params;
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

/// Creates piece of pipeline which performs memory efficient merging of partially aggregated data from several sources.
/// First processor will have num_inputs, last - single output. You should connect them to create pipeline.
Processors createMergingAggregatedMemoryEfficientPipe(
    Block header,
    AggregatingTransformParamsPtr params,
    size_t num_inputs,
    size_t num_merging_processors);

}

