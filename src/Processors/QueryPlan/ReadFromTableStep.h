#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

class ReadFromTableStep : public ISourceStep
{
public:
    ReadFromTableStep(
        SharedHeader header, String table_name_, TableExpressionModifiers table_expression_modifiers_, bool use_parallel_replicas_ = false);

    String getName() const override { return "ReadFromTable"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    const String & getTable() const { return table_name; }
    TableExpressionModifiers getTableExpressionModifiers() const { return table_expression_modifiers; }
    bool useParallelReplicas() const { return use_parallel_replicas; }
    bool & useParallelReplicas() { return use_parallel_replicas; }

    /** True when the query selects no columns from the table and the single output column was
      * injected by the planner purely to produce the correct number of rows (`SELECT count() FROM t`).
      * The injected column is chosen among the columns the current user may read, so it is not part
      * of the query's access contract: the query plan cache must not record it as a required column
      * of the cached plan's dependency (a hit re-check must apply the "any granted column" rule).
      * This is a store-time-only annotation consumed while collecting cache dependencies from a
      * freshly built plan; it is deliberately not serialized, so a deserialized step reports false.
      */
    bool readsOnlyInjectedColumn() const { return reads_only_injected_column; }
    void setReadsOnlyInjectedColumn(bool value) { reads_only_injected_column = value; }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    bool reads_only_injected_column = false;
};

}
