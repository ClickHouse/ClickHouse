#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedTransform : public IAccumulatingTransform
{
public:
    MergingAggregatedTransform(Block header_, AggregatingTransformParamsPtr params_, size_t max_threads_);
    String getName() const override { return "MergingAggregatedTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    AggregatingTransformParamsPtr params;
    Poco::Logger * log = &Poco::Logger::get("MergingAggregatedTransform");
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
