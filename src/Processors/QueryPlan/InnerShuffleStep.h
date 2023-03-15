#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/Pipe.h>
#include <base/types.h>
namespace DB
{
/**
 * For shuffling one block into blocks by a hash function. Each processor on the downstream
 * will handle different part of the data.
 */
class InnerShuffleStep : public ITransformingStep
{
public:
    explicit InnerShuffleStep(const DataStream & input_stream_, const std::vector<String> & hash_columns_);
    ~InnerShuffleStep() override = default;

    String getName() const override { return "InnerShuffle"; }
    // The shuffle buckets size is equal to pipeline's num_streams
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    std::vector<String> hash_columns; // columns' name to build the hash key

    void updateOutputStream() override;

};
}
