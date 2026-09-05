#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// `max_threads` is used to limit the number of threads for the result pipeline.
    /// `allow_narrowing` opts this step into the `max_streams_for_union_step` cap from
    /// `BuildQueryPipelineSettings`: it should only be set for steps that implement SQL
    /// `UNION ALL` / `UNION DISTINCT`. Other call sites (for example, `ClusterProxy` for
    /// distributed queries, `StorageBuffer`, `MergeTask`, projection optimizations) reuse
    /// `UnionStep` for plumbing and must not be narrowed, because shuffling streams via
    /// `ConcatProcessor` would break ordering invariants of downstream transforms such as
    /// `GroupingAggregatedTransform` for memory-efficient distributed aggregation.
    explicit UnionStep(SharedHeaders input_headers_, size_t max_threads_ = 0, bool allow_narrowing_ = false);

    String getName() const override { return "Union"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }
    bool isNarrowingAllowed() const { return allow_narrowing; }
    void disableNarrowing() { allow_narrowing = false; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;

    size_t max_threads;
    bool allow_narrowing;
};

}
