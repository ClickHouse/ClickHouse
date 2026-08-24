#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const SharedHeader & input_header_,
        size_t limit_, size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    size_t getLimit() const { return limit; }
    size_t getOffset() const { return offset; }

    size_t getLimitForSorting() const
    {
        if (limit > std::numeric_limits<UInt64>::max() - offset)
            return 0;

        return limit + offset;
    }

    bool withTies() const { return with_ties; }
    bool alwaysReadTillEnd() const { return always_read_till_end; }

    void markAsShardLimit() { is_shard_limit = true; }

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `LimitStep`,
    /// `ITransformingStep` and `IQueryPlanStep`:
    ///  - on the wire (written by `serialize`): `limit`, `offset`, `always_read_till_end`,
    ///    `with_ties`, and `description` (only when `with_ties`, which is exactly when it is read:
    ///    `LimitTransform`'s tie comparison is itself gated on `with_ties`).
    ///  - covered by the identity encoding itself: `output_header` (equal to `input_headers.front()`
    ///    for this pass-through step).
    ///  - extras: `is_shard_limit` - not on the wire, set post-construction via
    ///    `markAsShardLimit()`. `QueryPipeline::initRowsBeforeLimit` special-cases a shard limit so
    ///    rows it discards still count toward the parent limit's `rows_before_limit_at_least`, a
    ///    user-visible result field, so a shard limit is not interchangeable with a plain one.
    ///  - derived: `input_headers` - identical to `output_header` for this step.
    ///    `transform_traits` and `data_stream_traits` - computed from `getTraits` at construction
    ///    and never mutated. `collect_processors` - always default for this step.
    ///  - display or runtime instrumentation only: `step_description`, `step_index`, `processors`,
    ///    `dataflow_cache_updater`.
    bool supportsCascadesIdentity() const override { return isSerializable(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    bool hasCorrelatedExpressions() const override { return false; }

    bool supportsDataflowStatisticsCollection() const override { return true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t limit;
    size_t offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
    bool is_shard_limit = false;
};

}
