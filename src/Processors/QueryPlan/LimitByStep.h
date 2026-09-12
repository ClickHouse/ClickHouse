#pragma once
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct Settings;
struct QueryPlanSerializationSettings;

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    /// What the hash-based `LIMIT BY` needs to be able to spill its grouping state to disk.
    struct ExternalSettings
    {
        ExternalSettings() = default;
        explicit ExternalSettings(const Settings & settings);
        explicit ExternalSettings(const QueryPlanSerializationSettings & settings);

        void updatePlanSettings(QueryPlanSerializationSettings & settings) const;

        bool isEnabled() const
        {
            return max_bytes_in_state_before_external_limit_by != 0 || max_bytes_in_query_before_external_limit_by != 0;
        }

        /// The grouping state alone crossing this many bytes starts a spill.
        size_t max_bytes_in_state_before_external_limit_by = 0;
        /// The whole query crossing this many bytes starts a spill. Derived from the ratio setting.
        size_t max_bytes_in_query_before_external_limit_by = 0;
        double max_bytes_ratio_before_external_limit_by = 0;

        size_t max_block_size = 0;
        size_t min_free_disk_space = 0;
        String temporary_files_codec = "LZ4";
        size_t temporary_files_buffer_size = 0;
    };

    explicit LimitByStep(
            const SharedHeader & input_header_,
            size_t group_length_, size_t group_offset_, Names columns_,
            ExternalSettings external_settings_);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    bool isSerializable() const override { return true; }

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

    /// The hash-based `LIMIT BY`, spilling to disk when `can_spill` and the settings allow it.
    ProcessorPtr makeHashTransform(const SharedHeader & header, size_t length, size_t offset, bool can_spill) const;

    size_t group_length;
    size_t group_offset;

    Names columns;

    SortDescription sorted_columns_descr;

    ExternalSettings external_settings;

    bool skip_stream_merging = false;
};

}
