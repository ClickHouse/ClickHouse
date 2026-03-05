#include <Storages/ReadInOrderOptimizer.h>

#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_respect_aliases;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Finds expression like x = 'y' or f(x) = 'y',
/// where `x` is identifier, 'y' is literal and `f` is injective functions.
ASTPtr getFixedPoint(const ASTPtr & ast, const ContextPtr & context)
{
    const auto * func = ast->as<ASTFunction>();
    if (!func || func->name != "equals")
        return nullptr;

    if (!func->arguments || func->arguments->children.size() != 2)
        return nullptr;

    const auto & lhs = func->arguments->children[0];
    const auto & rhs = func->arguments->children[1];

    if (!lhs->as<ASTLiteral>() && !rhs->as<ASTLiteral>())
        return nullptr;

    /// Case of two literals doesn't make sense.
    if (lhs->as<ASTLiteral>() && rhs->as<ASTLiteral>())
        return nullptr;

    /// If indetifier is wrapped into injective functions, remove them.
    auto argument = lhs->as<ASTLiteral>() ? rhs : lhs;
    while (const auto * arg_func = argument->as<ASTFunction>())
    {
        if (!arg_func->arguments || arg_func->arguments->children.size() != 1)
            return nullptr;

        auto func_resolver = FunctionFactory::instance().tryGet(arg_func->name, context);
        if (!func_resolver || !func_resolver->isInjective({}))
            return nullptr;

        argument = arg_func->arguments->children[0];
    }

    return argument->as<ASTIdentifier>() ? argument : nullptr;
}

NameSet getFixedSortingColumns(
    const ASTSelectQuery & query, const Names & sorting_key_columns, const ContextPtr & context)
{
    ASTPtr condition;
    if (query.where() && query.prewhere())
        condition = makeASTFunction("and", query.where(), query.prewhere());
    else if (query.where())
        condition = query.where();
    else if (query.prewhere())
        condition = query.prewhere();

    if (!condition)
        return {};

    /// Convert condition to CNF for more convenient analysis.
    auto cnf = TreeCNFConverter::tryConvertToCNF(condition);
    if (!cnf)
        return {};

    NameSet fixed_points;
    NameSet sorting_key_columns_set(sorting_key_columns.begin(), sorting_key_columns.end());

    /// If we met expression like 'column = x', where 'x' is literal,
    /// in clause of size 1 in CNF, then we can guarantee
    /// that in all filtered rows 'column' will be equal to 'x'.
    cnf->iterateGroups([&](const auto & group)
    {
        if (group.size() == 1 && !group.begin()->negative)
        {
            auto fixed_point = getFixedPoint(group.begin()->ast, context);
            if (fixed_point)
            {
                auto column_name = fixed_point->getColumnName();
                if (sorting_key_columns_set.contains(column_name))
                    fixed_points.insert(column_name);
            }
        }
    });

    return fixed_points;
}

struct MatchResult
{
    /// One of {-1, 0, 1} - direction of the match. 0 means - doesn't match.
    int direction = 0;
    /// If true then current key must be the last in the matched prefix of sort description.
    bool is_last_key = false;
};

/// Optimize in case of exact match with order key element
/// or in some simple cases when order key element is wrapped into monotonic function.
MatchResult matchSortDescriptionAndKey(
    const ExpressionActions::Actions & actions,
    const SortColumnDescription & sort_column,
    const String & sorting_key_column)
{
    /// If required order depend on collation, it cannot be matched with primary key order.
    /// Because primary keys cannot have collations.
    if (sort_column.collator)
        return {};

    MatchResult result{sort_column.direction, false};

    /// For the path: order by (sort_column, ...)
    if (sort_column.column_name == sorting_key_column)
        return result;

    /// For the path: order by (function(sort_column), ...)
    /// Allow only one simple monotonic functions with one argument
    /// Why not allow multi monotonic functions?
    bool found_function = false;

    for (const auto & action : actions)
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        if (found_function)
            return {};

        found_function = true;
        if (action.node->children.size() != 1 || action.node->children.at(0)->result_name != sorting_key_column)
            return {};

        const auto & func = *action.node->function_base;
        if (!func.hasInformationAboutMonotonicity())
            return {};

        auto monotonicity = func.getMonotonicityForRange(*func.getArgumentTypes().at(0), {}, {});
        if (!monotonicity.is_monotonic)
            return {};

        /// If function is not strict monotonic, it can break order
        /// if it's not last in the prefix of sort description.
        /// E.g. if we have ORDER BY (d, u) -- ('2020-01-01', 1), ('2020-01-02', 0), ('2020-01-03', 1)
        /// ORDER BY (toStartOfMonth(d), u) -- ('2020-01-01', 1), ('2020-01-01', 0), ('2020-01-01', 1)
        if (!monotonicity.is_strict)
            result.is_last_key = true;

        if (!monotonicity.is_positive)
            result.direction *= -1;
    }

    if (!found_function)
        return {};

    return result;
}

}

ReadInOrderOptimizer::ReadInOrderOptimizer(
    const ASTSelectQuery & query_,
    const ManyExpressionActions & elements_actions_,
    const SortDescription & required_sort_description_,
    const TreeRewriterResultPtr & syntax_result)
    : elements_actions(elements_actions_)
    , required_sort_description(required_sort_description_)
    , query(query_)
{
    if (elements_actions.size() != required_sort_description.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sizes of sort description and actions are mismatched");

    /// Do not analyze joined columns.
    /// They may have aliases and come to description as is.
    /// We can mismatch them with order key columns at stage of fetching columns.
    forbidden_columns = syntax_result->getArrayJoinSourceNameSet();

    // array join result columns cannot be used in alias expansion.
    array_join_result_to_source = syntax_result->array_join_result_to_source;
}

InputOrderInfoPtr ReadInOrderOptimizer::getInputOrderImpl(
    const StorageMetadataPtr & metadata_snapshot,
    const SortDescription & description,
    const ManyExpressionActions & actions,
    const ContextPtr & context,
    UInt64 limit) const
{
    const Names & sorting_key_columns = metadata_snapshot->getSortingKeyColumns();
    int read_direction = description.at(0).direction;

    auto fixed_sorting_columns = getFixedSortingColumns(query, sorting_key_columns, context);

    SortDescription sort_description_for_merging;
    sort_description_for_merging.reserve(description.size());

    size_t desc_pos = 0;
    size_t key_pos = 0;

    while (desc_pos < description.size() && key_pos < sorting_key_columns.size())
    {
        if (forbidden_columns.contains(description[desc_pos].column_name))
            break;

        auto match = matchSortDescriptionAndKey(actions[desc_pos]->getActions(), description[desc_pos], sorting_key_columns[key_pos]);
        bool is_matched = match.direction && (desc_pos == 0 || match.direction == read_direction);

        if (!is_matched)
        {
            /// If one of the sorting columns is constant after filtering,
            /// skip it, because it won't affect order anymore.
            if (fixed_sorting_columns.contains(sorting_key_columns[key_pos]))
            {
                ++key_pos;
                continue;
            }

            break;
        }

        if (desc_pos == 0)
            read_direction = match.direction;

        sort_description_for_merging.push_back(description[desc_pos]);

        ++desc_pos;
        ++key_pos;

        if (match.is_last_key)
            break;
    }

    if (sort_description_for_merging.empty())
        return {};

    return std::make_shared<InputOrderInfo>(std::move(sort_description_for_merging), key_pos, read_direction, limit);
}

InputOrderInfoPtr ReadInOrderOptimizer::getInputOrder(
    const StorageMetadataPtr & metadata_snapshot, ContextPtr context, UInt64 limit) const
{
    if (!metadata_snapshot->hasSortingKey())
        return {};

    auto aliased_columns = metadata_snapshot->getColumns().getAliases();

    /// Replace alias column with proper expressions.
    /// Currently we only support alias column without any function wrapper,
    /// i.e.: `order by aliased_column` can have this optimization, but `order by function(aliased_column)` can not.
    /// This suits most cases.
    if (context->getSettingsRef()[Setting::optimize_respect_aliases] && !aliased_columns.empty())
    {
        SortDescription aliases_sort_description = required_sort_description;
        ManyExpressionActions aliases_actions = elements_actions;

        for (size_t i = 0; i < required_sort_description.size(); ++i)
        {
            if (!aliased_columns.contains(required_sort_description[i].column_name))
                continue;

            auto column_expr = metadata_snapshot->getColumns().get(required_sort_description[i].column_name).default_desc.expression->clone();
            replaceAliasColumnsInQuery(column_expr, metadata_snapshot->getColumns(), array_join_result_to_source, context);

            auto syntax_analyzer_result = TreeRewriter(context).analyze(column_expr, metadata_snapshot->getColumns().getAll());
            auto expression_analyzer = ExpressionAnalyzer(column_expr, syntax_analyzer_result, context);

            aliases_sort_description[i].column_name = column_expr->getColumnName();
            aliases_actions[i] = expression_analyzer.getActions(true);
        }

        return getInputOrderImpl(metadata_snapshot, aliases_sort_description, aliases_actions, context, limit);
    }

    return getInputOrderImpl(metadata_snapshot, required_sort_description, elements_actions, context, limit);
}

}
