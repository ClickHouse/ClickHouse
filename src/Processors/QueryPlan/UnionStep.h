#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct UnionWire;

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// `max_threads` is used to limit the number of threads for the result pipeline.
    /// `allow_narrowing` opts this step into the `max_streams_for_union_step` cap from
    /// `BuildQueryPipelineSettings`. Set it for a step that unites the branches a query asks for -
    /// SQL `UNION ALL` / `UNION DISTINCT`, and a `Merge` table expanded into the reads of its
    /// underlying tables (`ReadFromMerge::expandForParallelReplicas`), which is the same thing
    /// written differently. Other call sites (for example, `ClusterProxy` for distributed queries,
    /// `StorageBuffer`, `MergeTask`, projection optimizations) reuse `UnionStep` for plumbing and
    /// must not be narrowed, because shuffling streams via `ConcatProcessor` would break ordering
    /// invariants of downstream transforms such as `GroupingAggregatedTransform` for
    /// memory-efficient distributed aggregation.
    /// The flag is part of the plan, so it is serialized, and every node that executes a shipped
    /// fragment narrows the same unions the node that built it decided to narrow.
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

    /// The framed format: the wire struct is what the manifest in `UnionStep.cpp` declares.
    UnionWire toWire() const;
    static QueryPlanStepPtr fromWire(UnionWire wire, Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override;

    size_t max_threads;
    bool allow_narrowing;
};

/// What `UnionStep` puts on the wire in the framed format. `max_threads` is not on it: zero makes
/// the executing server derive it from its own settings, which is the right source for a per-machine
/// thread cap.
struct UnionWire
{
    /// Only the planner knows whether this union may be narrowed (SQL UNION) or feeds an
    /// order-sensitive consumer that forbids it.
    bool allow_narrowing = false;

    bool operator==(const UnionWire &) const = default;
};

}
