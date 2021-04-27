#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// TODO: add separate step for join.
class JoinStep : public IQueryPlanStep
{
public:
    explicit JoinStep(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        bool has_non_joined_rows_,
        size_t max_block_size_);

    String getName() const override { return "Join"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines, const BuildQueryPipelineSettings & settings) override;

    const JoinPtr & getJoin() const { return join; }

private:
    JoinPtr join;
    bool has_non_joined_rows;
    size_t max_block_size;
};

}
