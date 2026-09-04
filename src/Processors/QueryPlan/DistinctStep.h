#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct DistinctWire;

/// Whether adding a hashing preliminary DISTINCT can pay off, given the effective number of threads the
/// caller has already resolved. Such a step deduplicates each stream on its own so that the final,
/// single-stream DISTINCT has fewer rows left to merge, which takes a second stream to be worth
/// anything: at one thread it only hashes every row a second time.
bool preliminaryDistinctIsUseful(size_t max_threads);

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    DistinctStep(
        const SharedHeader & input_header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        /// If enabled, execute the `DISTINCT` for separate streams, otherwise for merged streams. The
        /// per-stream deduplication is best-effort: duplicates from different streams pass through it
        /// in any case, so a deduplicating consumer must follow, and on mostly-unique input the
        /// transform may abandon deduplication entirely (see `allow_preliminary_distinct_abandoning`).
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

    static QueryPlanStepPtr deserialize(Deserialization & ctx, bool pre_distinct_);
    static QueryPlanStepPtr deserializeNormal(Deserialization & ctx);
    static QueryPlanStepPtr deserializePre(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifests in `DistinctStep.cpp` declare,
    /// one per serialization name.
    DistinctWire toWire() const;
    static QueryPlanStepPtr fromWire(DistinctWire wire, Deserialization & ctx, bool pre_distinct_);

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

    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx, bool pre_distinct_);

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    SortDescription distinct_sort_desc;
    bool skip_stream_merging = false;
};

/// What `DistinctStep` puts on the wire in the framed format. The first four members are the
/// payload; the last three travel through the settings channel.
struct DistinctWire
{
    Names columns;
    /// Both DISTINCT transforms stop once this many distinct rows were produced.
    UInt64 limit_hint = 0;
    /// Selects the sorted-stream transform, which is correct only for an input sorted this way.
    SortDescription distinct_sort_desc;
    /// Lets the final DISTINCT skip the merge into one stream.
    bool skip_stream_merging = false;

    UInt64 max_rows = 0;
    UInt64 max_bytes = 0;
    OverflowMode overflow_mode = OverflowMode::THROW;

    bool operator==(const DistinctWire &) const = default;
};

}
