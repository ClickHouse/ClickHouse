#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    explicit UnionStep(Headers input_headers_, size_t max_threads_ = 0, bool parallel_replicas_ = false);

    String getName() const override { return "Union"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }
    bool parallelReplicas() const { return parallel_replicas; }
    bool & liftedUpDueToDistinct() { return lifted_up_due_to_distinct; }

    bool inOrderOptimizationBarrier() const { return parallel_replicas && lifted_up_due_to_distinct; }

    void serialize(Serialization & ctx) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override;

    const size_t max_threads = 0;
    const bool parallel_replicas = false;
    bool lifted_up_due_to_distinct = false;
};

}
