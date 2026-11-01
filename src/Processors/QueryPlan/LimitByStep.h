#pragma once
#include <Core/Defines.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct Settings;

/// Executes LIMIT BY for specified columns. See LimitByTransform.
class LimitByStep : public ITransformingStep
{
public:
    struct Settings
    {
        UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
        UInt64 prefer_external_sort_block_bytes = DEFAULT_BLOCK_SIZE * 256;

        /// The pipeline combines these thresholds into the smaller enabled spill trigger.
        /// Setting both to zero disables external `LIMIT BY`.
        UInt64 max_bytes_before_external_limit_by = 0;
        double max_bytes_ratio_before_external_limit_by = 0.;

        /// Internal steps serialize these settings through `updatePlanSettings` even when spilling is
        /// disabled. Defaults match the query settings, including the required nonzero buffer size.
        size_t min_free_disk_space = 0;
        String temporary_files_codec = "LZ4";
        UInt64 temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

        /// Disables external `LIMIT BY` for internal operations, such as decorrelated subqueries, that do not
        /// use query settings.
        Settings() = default;
        explicit Settings(const DB::Settings & settings_);
        explicit Settings(const QueryPlanSerializationSettings & settings_);

        void updatePlanSettings(QueryPlanSerializationSettings & plan_settings, UInt64 version) const;
    };

    explicit LimitByStep(
            const SharedHeader & input_header_,
            Settings settings_,
            size_t group_length_, size_t group_offset_, Names columns_, bool always_read_till_end_ = false);

    String getName() const override { return "LimitBy"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool hasCorrelatedExpressions() const override { return false; }

    void serializeSettings(QueryPlanSerializationSettings & plan_settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

    size_t getGroupLength() const { return group_length; }
    size_t getGroupOffset() const { return group_offset; }
    const Names & getColumns() const { return columns; }
    bool alwaysReadTillEnd() const { return always_read_till_end; }

    void applyOrder(const SortDescription & sort_description);

    /// Skip the resize-to-one-stream and run one `LimitByTransform` per input stream.
    /// Set by `optimizeLimitByPerPartition`; assumes upstream streams carry disjoint
    /// partition sets so no `LIMIT BY` group spans two streams.
    void skipStreamMerging() { skip_stream_merging = true; }

    /// The step must return the rows in their input order, which follows `input_order_`: it runs above the
    /// `ORDER BY` sorting of its query (set by the planners), or the optimizer propagates a global order
    /// through it, which the steps above may rely on (set by the `applyOrder` optimization). The in-memory
    /// `LIMIT BY` keeps the order by construction; a spilling one sorts the rows returned after spilling back
    /// into it (see `ExternalLimitByTransform`): by the columns of `input_order_` when the spilled rows carry
    /// them all, and by arrival numbers otherwise. Without an input order, the order has no meaning, so a
    /// spilling `LIMIT BY` may keep any rows of a group that fit its window, and returns them grouped by key.
    void preserveInputOrder(SortDescription input_order_) { input_order = std::move(input_order_); }
    bool preservesInputOrder() const { return !input_order.empty(); }

    /// Only the first `hint` rows of the output are read, by a `LIMIT` above the step (set by the
    /// `limitPushDown` optimization). A spilling `LIMIT BY` stops once its result has that many rows, and
    /// restores the input order of only that many rows after a spill. Zero disables the hint; of two hints,
    /// the smaller applies.
    void updateLimitHint(UInt64 hint);

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    Settings settings;

    size_t group_length;
    size_t group_offset;

    Names columns;

    bool always_read_till_end = false;

    SortDescription sorted_columns_descr;

    bool skip_stream_merging = false;
    /// The order that the output must keep; empty when the order has no meaning.
    SortDescription input_order;
    UInt64 limit_hint = 0;
};

}
