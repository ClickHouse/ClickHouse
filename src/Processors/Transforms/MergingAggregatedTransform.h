#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

namespace DB
{

using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;
using RemoteQueryExecutorPtrs = std::vector<RemoteQueryExecutorPtr>;
/** A pre-aggregate stream of blocks in which each block is already aggregated.
  * Aggregate functions in blocks should not be finalized so that their states can be merged.
  */
class MergingAggregatedTransform : public IAccumulatingTransform
{
public:
    MergingAggregatedTransform(
        Block header_,
        AggregatingTransformParamsPtr params_,
        size_t max_threads_,
        const SelectQueryInfo & query_info_,
        ContextPtr context_);

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

    const SelectQueryInfo & query_info;
    ContextPtr context;

    RemoteQueryExecutorPtrs remote_executors;

};

class MergingAggregatedOptimizedTransform : public IProcessor
{
public:
    MergingAggregatedOptimizedTransform(Block header_, AggregatingTransformParamsPtr params_, SortDescription description_, UInt64 limit_);

    String getName() const override { return "MergingAggregatedOptimizedTransform"; }

protected:
    Status prepare() override;
    void work() override;

private:
    void consume(Chunk chunk);
    void generate();

    Poco::Logger * log = &Poco::Logger::get("MergingAggregatedOptimizedTransform");

    Block header;
    AggregatingTransformParamsPtr params;
    SortDescription description;
    UInt64 limit;

    bool stop_reached = false;
    bool need_generate = false;
    bool is_consume_finished = false;

    Chunk top_chunk;
    Chunk current_chunk;

    AggregatedDataVariants data_variants;
    std::map<Int32, std::map<Int32, BlocksList>> blocks_by_levels;
};

}
