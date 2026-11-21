#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

class ReadFromTableStep : public ISourceStep
{
public:
    /// `header` is the header after `row_level_filter_`.
    ReadFromTableStep(
        SharedHeader header,
        String table_name_,
        TableExpressionModifiers table_expression_modifiers_,
        FilterDAGInfoPtr row_level_filter_ = nullptr,
        bool use_parallel_replicas_ = false);

    [[nodiscard]] String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    [[nodiscard]] bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    [[nodiscard]] const String & getTable() const { return table_name; }
    [[nodiscard]] TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    [[nodiscard]] const FilterDAGInfoPtr & getRowLevelFilter() const { return row_level_filter; }
    [[nodiscard]] bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }

    [[nodiscard]] QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    FilterDAGInfoPtr row_level_filter;
    bool use_parallel_replicas = false;
};

}
