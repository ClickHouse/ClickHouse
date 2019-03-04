#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

struct MergingAggregatedTransformParams
{
    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    MergingAggregatedTransformParams(const Aggregator::Params & params, bool final)
            : params(params), aggregator(params), final(final) {}

    Block getHeader() const { return aggregator.getHeader(final); }
};

using MergingAggregatedTransformParamsPtr = std::unique_ptr<MergingAggregatedTransformParams>;

/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedTransform : public IAccumulatingTransform
{
public:
    MergingAggregatedTransform(Block header, MergingAggregatedTransformParamsPtr params, size_t max_threads);
    String getName() const override { return "MergingAggregatedTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    MergingAggregatedTransformParamsPtr params;
    Logger * log = &Logger::get("MergingAggregatedTransform");
    size_t max_threads;

    AggregatedDataVariants data_variants;
    Aggregator::BucketToBlocks bucket_to_blocks;

    UInt64 total_input_rows = 0;
    UInt64 total_input_blocks = 0;

    BlocksList blocks;
    BlocksList::iterator next_block;

    bool consume_started = false;
    bool generate_started = false;
};

}
