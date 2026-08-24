#pragma once
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    explicit LimitByStep(
            const SharedHeader & input_header_,
            size_t group_length_, size_t group_offset_, Names columns_);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    /// Cascades cross-group identity. Field audit of every member of `LimitByStep`,
    /// `ITransformingStep` and `IQueryPlanStep`. Reachability is checked against the transforms
    /// `transformPipeline` builds - `LimitByTransform` (hash-based, needs one stream per key set) and
    /// `LimitBySortedStreamTransform` (range-based, needs the stream sorted by the keys).
    ///
    /// Own fields:
    ///  - `group_length`, `group_offset`, `columns` - on the wire (`serialize` writes all three).
    ///  - `sorted_columns_descr` - **extras**. Not on the wire (`deserialize` builds a step without it
    ///    and `applyOrder` is what installs it). Non-empty means every stream is sorted by the LIMIT BY
    ///    keys, which switches `transformPipeline` to `LimitBySortedStreamTransform` and, together with
    ///    `skip_stream_merging`, decides whether the pipeline is resized to one stream at all.
    ///  - `skip_stream_merging` - **extras**. Not on the wire. It asserts that the input streams carry
    ///    disjoint LIMIT BY key sets, so `transformPipeline` skips `pipeline.resize(1)` and runs one
    ///    transform per stream; substituting a step that does not make that assumption (or the other
    ///    way round) changes which rows survive.
    ///
    /// Inherited:
    ///  - `output_header` - covered by the identity encoding itself.
    ///  - `input_headers` - derived, excluded: `updateOutputHeader` copies the input header to the
    ///    output header, so the encoded output header is the input header.
    ///  - `transform_traits`, `data_stream_traits` - derived, excluded: computed by `getTraits` at
    ///    construction, never mutated.
    ///  - `collect_processors` - derived, excluded: always default for this step.
    ///  - `step_description`, `step_index`, `processors`, `dataflow_cache_updater` - display or
    ///    runtime instrumentation only, excluded.
    ///
    /// `isSerializable()` is unconditionally `true` and `serialize` writes only integers and strings,
    /// so it cannot throw for any instance.
    bool supportsCascadesIdentity() const override { return isSerializable(); }
    void appendCascadesIdentityExtras(CascadesIdentityExtras & extras) const override;

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    size_t getGroupLength() const { return group_length; }
    size_t getGroupOffset() const { return group_offset; }
    const Names & getColumns() const { return columns; }

    void applyOrder(const SortDescription & sort_description);

    /// Skip the resize-to-one-stream and run one `LimitByTransform` per input stream.
    /// Set by `optimizeLimitByPerPartition`; assumes upstream streams carry disjoint
    /// partition sets so no `LIMIT BY` group spans two streams.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    size_t group_length;
    size_t group_offset;

    Names columns;

    SortDescription sorted_columns_descr;

    bool skip_stream_merging = false;
};

}
