#include <memory>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

// template <typename... T>
// FMT_NODISCARD FMT_INLINE auto format(format_string<T...> fmt, T&&... args)
constexpr bool debug_logging_enabled = true;

template <typename... Args>
void logDebug(const char * format, Args &&... args)
{
    if constexpr (debug_logging_enabled)
    {
        LOG_DEBUG(&Poco::Logger::get("redundantDistinct"), format, args...);
    }
}

static std::set<std::string_view> getDistinctColumns(const DistinctStep * distinct)
{
    /// find non-const columns in DISTINCT
    const ColumnsWithTypeAndName & distinct_columns = distinct->getOutputStream().header.getColumnsWithTypeAndName();
    std::set<std::string_view> non_const_columns;
    for (const auto & column : distinct_columns)
    {
        if (!isColumnConst(*column.column))
            non_const_columns.emplace(column.name);
    }
    return non_const_columns;
}

size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/)
{
    if (parent_node->children.empty())
        return 0;

    /// check if it is preliminary distinct node
    QueryPlan::Node * distinct_node = nullptr;
    DistinctStep * distinct_step = typeid_cast<DistinctStep *>(parent_node->children.front()->step.get());
    if (!distinct_step)
        return 0;

    distinct_node = parent_node->children.front();

    const DistinctStep * inner_distinct_step = nullptr;
    QueryPlan::Node * node = distinct_node;
    while (!node->children.empty())
    {
        const IQueryPlanStep* current_step = node->step.get();

        /// don't try to remove DISTINCT after union or join
        if (typeid_cast<const UnionStep*>(current_step) || typeid_cast<const JoinStep*>(current_step))
            break;

        node = node->children.front();
        inner_distinct_step = typeid_cast<DistinctStep *>(node->step.get());
        if (inner_distinct_step)
            break;
    }
    if (!inner_distinct_step)
        return 0;

    /// possible cases (outer distinct -> inner distinct):
    /// final -> preliminary => do nothing
    /// preliminary -> final => try remove preliminary
    /// final -> final => try remove final
    /// preliminary -> preliminary => logical error?
    if (inner_distinct_step->isPreliminary())
        return 0;

    /// try to remove outer distinct step
    if (getDistinctColumns(distinct_step) != getDistinctColumns(inner_distinct_step))
        return 0;

    chassert(!distinct_node->children.empty());

    /// delete current distinct
    parent_node->children[0] = distinct_node->children.front();

    return 1;
}

}
