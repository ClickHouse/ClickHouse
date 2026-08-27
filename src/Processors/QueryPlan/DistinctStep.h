#pragma once
#include <Core/Defines.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

struct Settings;

/// Execute DISTINCT for specified columns.
class DistinctStep : public ITransformingStep
{
public:
    struct Settings
    {
        /// Restrictions on the maximum size of the DISTINCT set.
        SizeLimits set_size_limits;

        UInt64 max_block_size = DEFAULT_BLOCK_SIZE;

        /// The external DISTINCT thresholds. The effective limit is their min-combination, computed when
        /// the pipeline is built (both zero - external DISTINCT is disabled).
        UInt64 max_bytes_before_external_distinct = 0;
        double max_bytes_ratio_before_external_distinct = 0.;

        /// The spill-related members are consumed only when external DISTINCT is enabled, but they
        /// must hold valid values even in the default-constructed struct: internal DISTINCT steps
        /// built with it (e.g. the set transfer of a distributed plan) are serialized through
        /// updatePlanSettings, and e.g. the buffer size is a non-zero plan setting. The defaults
        /// mirror the defaults of the corresponding query settings.
        size_t min_free_disk_space = 0;
        String temporary_files_codec = "LZ4";
        UInt64 temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

        /// External DISTINCT disabled (e.g. deduplication during merges, which must not depend on
        /// query-level settings or query memory tracking).
        Settings() = default;
        explicit Settings(const DB::Settings & settings_);
        explicit Settings(const QueryPlanSerializationSettings & settings_);

        void updatePlanSettings(QueryPlanSerializationSettings & plan_settings) const;
    };

    DistinctStep(
        const SharedHeader & input_header_,
        Settings settings_,
        UInt64 limit_hint_,
        const Names & columns_,
        /// If is enabled, execute distinct for separate streams, otherwise for merged streams.
        bool pre_distinct_);

    String getName() const override { return "Distinct"; }
    const Names & getColumnNames() const { return columns; }

    String getSerializationName() const override { return pre_distinct ? "PreDistinct" : "Distinct"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & format_settings) const override;

    bool isPreliminary() const { return pre_distinct; }

    UInt64 getLimitHint() const { return limit_hint; }
    void updateLimitHint(UInt64 hint);

    void serializeSettings(QueryPlanSerializationSettings & plan_settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx, bool pre_distinct_);
    static QueryPlanStepPtr deserializeNormal(Deserialization & ctx);
    static QueryPlanStepPtr deserializePre(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    const Settings & getSettings() const { return settings; }

    void applyOrder(SortDescription sort_desc) { distinct_sort_desc = std::move(sort_desc); }
    const SortDescription & getSortDescription() const override { return distinct_sort_desc; }

    /// Each input stream contains a disjoint set of the DISTINCT key values (e.g. because each stream
    /// corresponds to a separate partition and the partition key is a function of the DISTINCT columns).
    /// In that case the final DISTINCT can deduplicate every stream independently and skip merging them
    /// into a single stream.
    void skipStreamMerging() { skip_stream_merging = true; }

private:
    void updateOutputHeader() override;

    Settings settings;
    UInt64 limit_hint;
    const Names columns;
    bool pre_distinct;
    SortDescription distinct_sort_desc;
    bool skip_stream_merging = false;
};

}
