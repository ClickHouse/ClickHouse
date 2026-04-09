#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedTransform : public IAccumulatingTransform
{
public:
    MergingAggregatedTransform(SharedHeader header_, Aggregator::Params params_, bool final_, GroupingSetsParamsList grouping_sets_params);

    ~MergingAggregatedTransform() override;

    String getName() const override { return "MergingAggregatedTransform"; }

    static Block appendGroupingIfNeeded(const Block & in_header, Block out_header);

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    LoggerPtr log = getLogger("MergingAggregatedTransform");

    struct GroupingSet
    {
        Aggregator::BucketToChunks bucket_to_chunks;
        ExpressionActionsPtr reordering_key_columns_actions;
        ExpressionActionsPtr creating_missing_keys_actions;
        AggregatingTransformParamsPtr params;
    };

    using GroupingSets = std::vector<GroupingSet>;
    GroupingSets grouping_sets;

    UInt64 total_input_rows = 0;
    UInt64 total_input_blocks = 0;

    Aggregator::AggregatedChunks chunks;
    Aggregator::AggregatedChunks::iterator next_chunk;

    bool consume_started = false;
    bool generate_started = false;

    void addChunk(Columns columns, size_t num_rows, Int32 bucket_num, bool is_overflows);
};

}
