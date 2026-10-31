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

    /** The column set whose `SELECT` privilege the planner actually verified for this read, i.e.
      * the columns the query selects from the table expression, including `ALIAS` columns.
      * It differs from the step's output header, which carries the *physical* columns the storage
      * reads: for `b UInt64 ALIAS a + 1`, `SELECT b FROM t` checks `SELECT(b)` but reads `a`.
      * The query plan cache must record this logical set as the dependency's columns, so that a
      * cache hit rechecks exactly the grants a miss would check - recording the physical columns
      * would let a hit serve `b` after `REVOKE SELECT(b)` (and falsely deny users who only hold
      * the grant on the alias).
      * Like `readsOnlyInjectedColumn`, this is a store-time-only annotation consumed while
      * collecting cache dependencies from a freshly built plan; it is deliberately not serialized,
      * so a deserialized step reports an empty set.
      */
    const Names & getAccessCheckedColumns() const { return access_checked_columns; }
    void setAccessCheckedColumns(Names value) { access_checked_columns = std::move(value); }

    /** The row-level security filter of the table, set only in a plan built for the query plan cache.
      * Regular planning hands the policy to `IStorage::read` as `SelectQueryInfo::row_level_filter`, so a
      * storage that supports it (e.g. `MergeTree`) applies it inside the read, before any user expression.
      * An explicit `FilterStep` above the read would not be such a boundary: plan optimizations may merge
      * it with the user's filters (`mergeFilters`), so a user expression such as `throwIf` could be evaluated
      * on the rows the policy hides. The filter is carried here and pushed into the read when the plan is
      * materialized (see `QueryPlan::resolveStorages`).
      */
    const FilterDAGInfoPtr & getRowLevelFilter() const { return row_level_filter; }
    void setRowLevelFilter(FilterDAGInfoPtr value) { row_level_filter = std::move(value); }

    QueryPlanStepPtr clone() const override;
private:
    String table_name;
    TableExpressionModifiers table_expression_modifiers;
    bool use_parallel_replicas = false;
    bool reads_only_injected_column = false;
    Names access_checked_columns;
    FilterDAGInfoPtr row_level_filter;
};

}
