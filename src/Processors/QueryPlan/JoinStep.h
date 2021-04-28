#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// TODO: add separate step for join.
class JoinStep : public IQueryPlanStep
{
public:
    JoinStep(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        size_t max_block_size_);

    String getName() const override { return "Join"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

private:
    JoinPtr join;
    size_t max_block_size;
    Processors processors;
};

class StorageJoinStep : public ITransformingStep
{
public:
    StorageJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_);

    String getName() const override { return "StorageJoin"; }
    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

private:
    JoinPtr join;
    size_t max_block_size;
};

}
