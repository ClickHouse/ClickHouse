#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableStep : public ISourceStep
{
public:
    /// Whether the plan applies the table's SELECT row policy: `FilterStep` - a filter step above this read
    /// does; `NotInPlan` - no step does (the sender pushed the policy into the read, which is not serialized,
    /// or had none); `Unknown` - the sender did not record it.
    enum class RowPolicyPlacement : uint8_t
    {
        Unknown,
        NotInPlan,
        FilterStep,
    };

    /// An `additional_table_filters` entry that the filter step `steps_above` steps above this read applies, with the
    /// filter column of that step. The table alias is not stored. The query text also names the table `__tableN` on the
    /// executing node, thus an entry there keyed by the initial alias does not match in either mode.
    struct AdditionalFilter
    {
        String text;
        String filter_column;
        UInt64 steps_above = 0;
    };

    ReadFromTableStep(
        SharedHeader header,
        String table_name_,
        TableExpressionModifiers table_expression_modifiers_,
        bool use_parallel_replicas_ = false,
        RowPolicyPlacement row_policy_placement_ = RowPolicyPlacement::Unknown,
        std::optional<Names> selected_columns_ = {},
        std::optional<size_t> columns_to_read_ = {},
        std::optional<AdditionalFilter> additional_filter_ = {},
        UInt64 filter_steps_ = 0);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }
    RowPolicyPlacement getRowPolicyPlacement() const { return row_policy_placement; }
    /// The columns of the table that the query uses and the sender authorized; the header also has the columns that
    /// only the filters above this read need. Not set when the sender did not record them.
    const std::optional<Names> & getSelectedColumns() const { return selected_columns; }
    /// The number of columns the sender checked against `max_columns_to_read`, without those only the filters need.
    std::optional<size_t> getColumnsToRead() const { return columns_to_read; }
    const std::optional<AdditionalFilter> & getAdditionalFilter() const { return additional_filter; }
    /// The number of steps right above this read that belong to it (ALIAS columns, PREWHERE, the policy, the custom key
    /// filter), after which the sender applies an `additional_table_filters` entry.
    UInt64 getFilterSteps() const { return filter_steps; }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    RowPolicyPlacement row_policy_placement = RowPolicyPlacement::Unknown;
    std::optional<Names> selected_columns;
    std::optional<size_t> columns_to_read;
    std::optional<AdditionalFilter> additional_filter;
    UInt64 filter_steps = 0;
};

}
