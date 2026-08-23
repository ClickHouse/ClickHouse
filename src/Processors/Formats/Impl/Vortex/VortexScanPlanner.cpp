#include <Processors/Formats/Impl/Vortex/VortexScanPlanner.h>

#if USE_VORTEX

#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFilterInfo.h>
#include <Processors/Formats/Impl/Vortex/VortexExpressionConverter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <arrow/type.h>
#include <fmt/ranges.h>

#include <unordered_set>

#include <vortex_ffi.h>

namespace ProfileEvents
{
extern const Event VortexFilterPushdownConjunctsPushed;
extern const Event VortexFilterPushdownConjunctsDropped;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace DB::Vortex
{

namespace
{

/// Translates the filter DAG into a Vortex expression: NOTs are pushed into the atoms first, then
/// every top-level conjunct is translated on its own, and the ones that do not translate are
/// dropped - the scan may keep more rows than the condition, never fewer, and ClickHouse reapplies
/// the full WHERE afterwards.
VortexExpressionPtr buildFilter(
    const Block & header,
    const arrow::Schema & file_schema,
    const FormatFilterInfo & filter_info,
    const FormatSettings & format_settings,
    VortexScanPlan & plan)
{
    ContextPtr context = filter_info.context.lock();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Context has expired");

    ActionsDAGWithInversionPushDown inverted(filter_info.filter_actions_dag->getOutputs().front(), context, /* boolean_context */ true);
    if (!inverted.predicate)
        return nullptr;

    RPNBuilderTreeContext tree_context(context);

    /// The top-level conjuncts, in their original order.
    std::vector<const ActionsDAG::Node *> conjuncts;
    std::vector<const ActionsDAG::Node *> stack{inverted.predicate};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        RPNBuilderTreeNode tree_node(node, tree_context);
        if (auto function_node = tree_node.toFunctionNodeOrNull(); function_node && function_node->getFunctionName() == "and")
        {
            for (size_t i = function_node->getArgumentsSize(); i > 0; --i)
                stack.push_back(function_node->getArgumentAt(i - 1).getDAGNode());
            continue;
        }
        conjuncts.push_back(node);
    }

    VortexExpressionConverter converter(header, file_schema, format_settings);
    VortexExpressionPtr filter;
    for (const auto * conjunct : conjuncts)
    {
        auto expression = converter.tryConvert(RPNBuilderTreeNode(conjunct, tree_context), /* allow_widening */ true);
        if (!expression)
            continue;
        ++plan.filter_conjuncts_pushed;
        if (filter)
            filter = VortexExpressionPtr(vortex_ffi_expr_and(filter.get(), expression.get()));
        else
            filter = std::move(expression);
    }
    plan.filter_conjuncts_total = conjuncts.size();
    return filter;
}

}

VortexScanPlan planVortexScan(
    const Block & header,
    const arrow::Schema & file_schema,
    const FormatFilterInfo * filter_info,
    const FormatSettings & format_settings,
    const LoggerPtr & log)
{
    VortexScanPlan plan;

    /// Request the header's columns that the file actually has; the rest are filled with default
    /// values, and if none is left there is no scan at all, only a row count.
    ///
    /// A header entry can also name a subcolumn, `name.sub`, which `ArrowColumnToCHColumn` can only
    /// extract once the whole field `name` has been read - hence the second name per column.
    std::unordered_set<std::string> added_column_names;
    auto add_column_name = [&](const std::string & name)
    {
        if (file_schema.GetFieldByName(name) && added_column_names.emplace(name).second)
            plan.column_names.push_back(name);
    };
    for (const auto & column : header)
    {
        add_column_name(column.name);
        add_column_name(Nested::extractTableName(column.name));
    }

    if (!plan.column_names.empty() && format_settings.vortex.filter_push_down && filter_info && filter_info->hasFilter())
    {
        plan.filter = buildFilter(header, file_schema, *filter_info, format_settings, plan);
        ProfileEvents::increment(ProfileEvents::VortexFilterPushdownConjunctsPushed, plan.filter_conjuncts_pushed);
        ProfileEvents::increment(
            ProfileEvents::VortexFilterPushdownConjunctsDropped, plan.filter_conjuncts_total - plan.filter_conjuncts_pushed);
    }

    LOG_TEST(
        log,
        "Vortex scan plan: columns [{}], filter: {} ({} of {} conjuncts pushed)",
        fmt::join(plan.column_names, ", "),
        vortexExpressionToString(plan.filter.get()),
        plan.filter_conjuncts_pushed,
        plan.filter_conjuncts_total);

    return plan;
}

}

#endif
