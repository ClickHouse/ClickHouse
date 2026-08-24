#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
        const SharedHeader & input_header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        /// If is enabled, execute distinct for separate streams, otherwise for merged streams.
        bool pre_distinct_);

    String getName() const override { return "Distinct"; }
    const Names & getColumnNames() const { return columns; }

    String getSerializationName() const override { return pre_distinct ? "PreDistinct" : "Distinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool isPreliminary() const { return pre_distinct; }

    UInt64 getLimitHint() const { return limit_hint; }
    void updateLimitHint(UInt64 hint);

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `DistinctStep`,
    /// `ITransformingStep` and `IQueryPlanStep`. Reachability is checked against the transforms
    /// `transformPipeline` builds - `DistinctTransform` (hash set over all `columns`) and
    /// `DistinctSortedStreamTransform` (range-based, needs the stream sorted by a prefix).
    ///
    /// Own fields:
    ///  - `columns` - on the wire (`serialize`).
    ///  - `set_size_limits` - on the wire: `serializeSettings` writes all three of `max_rows`,
    ///    `max_bytes` and `overflow_mode`, which is every member of `SizeLimits`.
    ///  - `pre_distinct` - covered by the identity encoding itself: it selects
    ///    `getSerializationName()` (`"PreDistinct"` vs `"Distinct"`), which the encoding writes first.
    ///    It is not a `serialize` payload but a per-name `deserialize` entry point.
    ///  - `limit_hint` - **extras**. `serialize` deliberately skips it ("Let's not serialize
    ///    limit_hint"). Both transforms take it and stop consuming once that many distinct rows were
    ///    produced, so a step with a hint can emit fewer rows than one without.
    ///  - `distinct_sort_desc` - **extras**. Not on the wire (`applyOrder` installs it after
    ///    construction). Non-empty switches `transformPipeline` to `DistinctSortedStreamTransform`,
    ///    which deduplicates by ranges of the sorted prefix and is only correct for an input actually
    ///    sorted that way. It is also this step's `getSortDescription`, i.e. what the optimizer
    ///    believes about the output order.
    ///  - `skip_stream_merging` - **extras**. Not on the wire. It encodes the assumption that the
    ///    input streams hold disjoint DISTINCT key sets, so the final DISTINCT skips
    ///    `pipeline.resize(1)`; substituting a step that does not make that assumption changes which
    ///    rows survive.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: `updateOutputHeader` copies the input header to the
    ///    output header, so the encoded output header is the input header.
    ///  - `transform_traits`, `data_stream_traits` - derived, excluded: computed by `getTraits` from
    ///    `pre_distinct` at construction, never mutated.
    ///  - `collect_processors` - derived, excluded: always default for this step.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    ///
    /// `isSerializable()` is unconditionally `true` and `serialize` writes only strings, so it cannot
    /// throw for any instance.
    bool supportsCascadesIdentity() const override { return isSerializable(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx, bool pre_distinct_);
    static QueryPlanStepPtr deserializeNormal(Deserialization & ctx);
    static QueryPlanStepPtr deserializePre(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }

    void applyOrder(SortDescription sort_desc) { distinct_sort_desc = std::move(sort_desc); }
    const SortDescription & getSortDescription() const override { return distinct_sort_desc; }

    /// Each input stream contains a disjoint set of the DISTINCT key values (e.g. because each stream
    /// corresponds to a separate partition and the partition key is a function of the DISTINCT columns).
    /// In that case the final DISTINCT can deduplicate every stream independently and skip merging them
    /// into a single stream.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    void updateOutputHeader() override;

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    SortDescription distinct_sort_desc;
    bool skip_stream_merging = false;
};

}
