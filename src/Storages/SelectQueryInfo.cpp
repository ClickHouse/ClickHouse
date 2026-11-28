#include <Interpreters/Set.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/SelectQueryInfo.h>
#include <Processors/QueryPlan/Serialization.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <algorithm>

namespace DB
{

SelectQueryInfo::SelectQueryInfo()
    : prepared_sets(std::make_shared<PreparedSets>())
{}

/// Whether a read limit with a non-throwing overflow mode can end the read early instead of failing the
/// query. Row and byte limits, including the leaf ones, are checked against the progress of the whole query
/// and `max_execution_time` against the elapsed time, so the read can stop however few rows it selects.
/// These are the limits that the sources of the read enforce; leaf limits are set only on a leaf of a distributed
/// query (`distributed_depth > 0`).
bool SelectQueryInfo::mayStopReadingEarly() const
{
    if (!storage_limits)
        return false;
    return std::ranges::any_of(
        *storage_limits,
        [](const StorageLimits & limits)
        {
            const auto & local_limits = limits.local_limits;
            return (local_limits.size_limits.overflow_mode != OverflowMode::THROW && local_limits.size_limits.hasLimits())
                || (limits.leaf_limits.overflow_mode != OverflowMode::THROW && limits.leaf_limits.hasLimits())
                || (local_limits.timeout_overflow_mode != OverflowMode::THROW
                    && local_limits.speed_limits.max_execution_time.totalMicroseconds() != 0);
        });
}

bool SelectQueryInfo::isFinal() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->hasFinal();

    if (query_tree && !apply_query_level_final_if_no_modifiers)
        return false;

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}

bool SelectQueryInfo::isStream() const
{
    return table_expression_modifiers && table_expression_modifiers->hasStream();
}

/// With a query tree, every table expression carries its own modifiers, while the query AST holds
/// only the `SAMPLE` of the left-most table. A table expression without modifiers is not sampled.
std::optional<TableExpressionModifiers::Rational> SelectQueryInfo::getSampleSizeRatio() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->getSampleSizeRatio();

    if (query_tree)
        return {};

    if (auto sample_size = query->as<ASTSelectQuery &>().sampleSize())
        return sample_size->as<ASTSampleRatio &>().ratio;
    return {};
}

std::optional<TableExpressionModifiers::Rational> SelectQueryInfo::getSampleOffsetRatio() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->getSampleOffsetRatio();

    if (query_tree)
        return {};

    if (auto sample_offset = query->as<ASTSelectQuery &>().sampleOffset())
        return sample_offset->as<ASTSampleRatio &>().ratio;
    return {};
}

UnorderedMapWithMemoryTracking<std::string, ColumnWithTypeAndName> SelectQueryInfo::buildNodeNameToInputNodeColumn() const
{
    UnorderedMapWithMemoryTracking<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (planner_context)
    {
        auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            /// ALIAS columns cannot be used in the filter expression without being calculated in ActionsDAG,
            /// so they should not be added to the input nodes.
            if (table_expression_data.hasAliasColumn(column_name))
                continue;
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(nullptr, column.type, column_name));
        }
    }
    return node_name_to_input_node_column;
}

PrewhereInfo PrewhereInfo::clone() const
{
    PrewhereInfo prewhere_info;

    prewhere_info.prewhere_actions = prewhere_actions.clone();
    prewhere_info.prewhere_column_name = prewhere_column_name;
    prewhere_info.remove_prewhere_column = remove_prewhere_column;
    prewhere_info.need_filter = need_filter;

    return prewhere_info;
}

void PrewhereInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    prewhere_actions.serialize(ctx.out, ctx.registry);
    writeStringBinary(prewhere_column_name, ctx.out);
    writeBinary(remove_prewhere_column, ctx.out);
    writeBinary(need_filter, ctx.out);
}

PrewhereInfo PrewhereInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    PrewhereInfo prewhere_info;

    prewhere_info.prewhere_actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);
    readStringBinary(prewhere_info.prewhere_column_name, ctx.in);
    readBinary(prewhere_info.remove_prewhere_column, ctx.in);
    readBinary(prewhere_info.need_filter, ctx.in);

    return prewhere_info;
}

void FilterDAGInfo::serialize(IQueryPlanStep::Serialization & ctx) const
{
    actions.serialize(ctx.out, ctx.registry);
    writeStringBinary(column_name, ctx.out);
    writeBinary(do_remove_column, ctx.out);
}

FilterDAGInfo FilterDAGInfo::deserialize(IQueryPlanStep::Deserialization & ctx)
{
    FilterDAGInfo filter_dag_info;

    filter_dag_info.actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);
    readStringBinary(filter_dag_info.column_name, ctx.in);
    readBinary(filter_dag_info.do_remove_column, ctx.in);

    return filter_dag_info;
}

}
