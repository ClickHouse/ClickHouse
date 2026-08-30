#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

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
        /// If is enabled, execute distinct for separate streams, otherwise for merged streams.
        bool pre_distinct_,
        /// A downstream limit consumes the current stream order but cannot be used as `limit_hint`.
        bool has_order_sensitive_post_distinct_limit_ = false);

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

    QueryPlanStepPtr clone() const override;

    const SizeLimits & getSetSizeLimits() const { return set_size_limits; }

    void applyOrder(SortDescription sort_desc) { distinct_sort_desc = std::move(sort_desc); }
    const SortDescription & getSortDescription() const override { return distinct_sort_desc; }

    /// Each input stream contains a disjoint set of the DISTINCT key values (e.g. because each stream
    /// corresponds to a separate partition and the partition key is a function of the DISTINCT columns).
    /// In that case the final DISTINCT can deduplicate every stream independently and skip merging them
    /// into a single stream.
    void skipStreamMerging() { skip_stream_merging = true; }

    /// When the input streams are not disjoint, they can be made so: repartitioning them by the hash of
    /// the DISTINCT columns sends equal key values into the same stream, so the deduplication runs in
    /// parallel instead of on a single thread. It reorders the output, so it may only be enabled when
    /// nothing downstream relies on the order of this step - see `applyOrder`.
    void enableParallelDistinct() { parallel_distinct = true; }

    /// A deserialized step cannot know whether the order of its output is consumed downstream, see
    /// `order_guard_state_is_known`.
    void forgetOrderGuardState() { order_guard_state_is_known = false; }
    bool isOrderGuardStateKnown() const { return order_guard_state_is_known; }

private:
    void updateOutputHeader() override;

    /// Repartitions the pipeline by the hash of the DISTINCT columns. Returns `false` if it did not,
    /// in which case the streams still have to be merged into one before deduplicating.
    bool scatterStreamsByHash(QueryPipelineBuilder & pipeline) const;

    SizeLimits set_size_limits;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    bool has_order_sensitive_post_distinct_limit;
    SortDescription distinct_sort_desc;
    bool skip_stream_merging = false;
    bool parallel_distinct = false;

    /// `limit_hint` and `has_order_sensitive_post_distinct_limit` are not serialized, and a serialized
    /// fragment is optimized again on the worker, so a deserialized step would decide whether to keep
    /// its input in more than one stream without knowing that the initiator kept the stream single for
    /// a downstream `LIMIT`, `OFFSET`, or `LIMIT BY`. Fail close: a step that lost that state neither
    /// scatters by hash nor skips the merge of already-disjoint streams.
    bool order_guard_state_is_known = true;
};

}
