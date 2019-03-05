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
    GroupingAggregatedTransform(const Block & header, size_t num_inputs, AggregatingTransformParamsPtr params);

    /// Special setting: in case if single source can return several chunks with same bucket.
    void allowSeveralChunksForSingleBucketPerSource() { expect_several_chunks_for_single_bucket_per_source = true; }

protected:
    Status prepare() override;
    void work() override;

private:
    size_t num_inputs;
    AggregatingTransformParamsPtr params;

    std::vector<Int32> last_bucket_number;
    std::map<Int32, Chunks> chunks;
    Chunks overflow_chunks;
    Chunks single_level_chunks;
    Int32 current_bucket = 0;
    Int32 next_bucket_to_push = 0; /// Always <= current_bucket.
    bool has_two_level = false;

    bool all_inputs_finished = false;
    bool read_from_all_inputs = false;
    std::vector<bool> read_from_input;

    bool expect_several_chunks_for_single_bucket_per_source = false;

    void addChunk(Chunk chunk, size_t input);
    void readFromAllInputs();
    bool tryPushSingleLevelData();
    bool tryPushTwoLevelData();
    bool tryPushOverflowData();
    void pushData(Chunks chunks, Int32 bucket, bool is_overflows);
};

/// Merge aggregated data from single bucket.
class MergingAggregatedBucketTransform : public ISimpleTransform
{
public:
    explicit MergingAggregatedBucketTransform(AggregatingTransformParamsPtr params);

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
    Status prepare() override;

private:
    size_t num_inputs;
    AggregatingTransformParamsPtr params;
    std::vector<Int32> last_bucket_number;
    std::map<Int32, Chunk> chunks;
    Chunk overflow_chunk;

    bool tryPushChunk();
    void addChunk(Chunk chunk);
};

/// Creates piece of pipeline which performs memory efficient merging of partially aggregated data from several sources.
/// First processor will have num_inputs, last - single output. You should connect them to create pipeline.
Processors createMergingAggregatedMemoryEfficientPipe(
    Block header,
    AggregatingTransformParamsPtr params,
    size_t num_inputs,
    size_t num_merging_processors)
{
    Processors processors;
    processors.reserve(num_merging_processors + 2);

    auto grouping = std::make_shared<GroupingAggregatedTransform>(header, num_inputs, params);
    processors.emplace_back(std::move(grouping));

    if (num_merging_processors <= 1)
    {
        /// --> GroupingAggregated --> MergingAggregatedBucket -->
        auto transform = std::make_shared<MergingAggregatedBucketTransform>(params);
        connect(processors.back()->getOutputs().front(), transform->getInputPort());

        processors.emplace_back(std::move(transform));
        return processors;
    }

    /// -->                                        --> MergingAggregatedBucket -->
    /// --> GroupingAggregated --> ResizeProcessor --> MergingAggregatedBucket --> SortingAggregated -->
    /// -->                                        --> MergingAggregatedBucket -->

    auto resize = std::make_shared<ResizeProcessor>(header, 1, num_merging_processors);
    connect(processors.back()->getOutputs().front(), resize->getInputs().front());
    processors.emplace_back(std::move(resize));

    auto sorting = std::make_shared<SortingAggregatedTransform>(num_merging_processors, params);
    auto out = processors.back()->getOutputs().begin();
    auto in = sorting->getInputs().begin();

    for (size_t i = 0; i < num_merging_processors; ++i, ++in, ++out)
    {
        auto transform = std::make_shared<MergingAggregatedBucketTransform>(params);
        connect(*out, transform->getInputPort());
        connect(transform->getOutputPort(), *in);
        processors.emplace_back(std::move(transform));
    }

    processors.emplace_back(std::move(sorting));
    return processors;
}

}

