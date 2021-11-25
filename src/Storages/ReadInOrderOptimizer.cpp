#include <Storages/ReadInOrderOptimizer.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Functions/IFunction.h>
#include <Interpreters/TableJoin.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ReadInOrderOptimizer::ReadInOrderOptimizer(
    const ManyExpressionActions & elements_actions_,
    const SortDescription & required_sort_description_,
    const TreeRewriterResultPtr & syntax_result)
    : elements_actions(elements_actions_)
    , required_sort_description(required_sort_description_)
{
    if (elements_actions.size() != required_sort_description.size())
        throw Exception("Sizes of sort description and actions are mismatched", ErrorCodes::LOGICAL_ERROR);

    /// Do not analyze joined columns.
    /// They may have aliases and come to description as is.
    /// We can mismatch them with order key columns at stage of fetching columns.
    forbidden_columns = syntax_result->getArrayJoinSourceNameSet();

    // array join result columns cannot be used in alias expansion.
    array_join_result_to_source = syntax_result->array_join_result_to_source;
}

InputOrderInfoPtr ReadInOrderOptimizer::getInputOrder(const StorageMetadataPtr & metadata_snapshot, ContextPtr context) const
{
    Names sorting_key_columns = metadata_snapshot->getSortingKeyColumns();
    if (!metadata_snapshot->hasSortingKey())
        return {};

    SortDescription order_key_prefix_descr;
    int read_direction = required_sort_description.at(0).direction;

    size_t prefix_size = std::min(required_sort_description.size(), sorting_key_columns.size());
    auto aliased_columns = metadata_snapshot->getColumns().getAliases();

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (forbidden_columns.count(required_sort_description[i].column_name))
            break;

        /// Optimize in case of exact match with order key element
        ///  or in some simple cases when order key element is wrapped into monotonic function.
        auto apply_order_judge = [&] (const ExpressionActions::Actions & actions, const String & sort_column)
        {
            /// If required order depend on collation, it cannot be matched with primary key order.
            /// Because primary keys cannot have collations.
            if (required_sort_description[i].collator)
                return false;

            int current_direction = required_sort_description[i].direction;
            /// For the path: order by (sort_column, ...)
            if (sort_column == sorting_key_columns[i] && current_direction == read_direction)
            {
                return true;
            }
            /// For the path: order by (function(sort_column), ...)
            /// Allow only one simple monotonic functions with one argument
            /// Why not allow multi monotonic functions?
            else
            {
                bool found_function = false;

                for (const auto & action : actions)
                {
                    if (action.node->type != ActionsDAG::ActionType::FUNCTION)
                    {
                        continue;
                    }

                    if (found_function)
                    {
                        current_direction = 0;
                        break;
                    }
                    else
                        found_function = true;

                    if (action.node->children.size() != 1 || action.node->children.at(0)->result_name != sorting_key_columns[i])
                    {
                        current_direction = 0;
                        break;
                    }

                    const auto & func = *action.node->function_base;
                    if (!func.hasInformationAboutMonotonicity())
                    {
                        current_direction = 0;
                        break;
                    }

                    auto monotonicity = func.getMonotonicityForRange(*func.getArgumentTypes().at(0), {}, {});
                    if (!monotonicity.is_monotonic)
                    {
                        current_direction = 0;
                        break;
                    }
                    else if (!monotonicity.is_positive)
                        current_direction *= -1;
                }

                if (!found_function)
                    current_direction = 0;

                if (!current_direction || (i > 0 && current_direction != read_direction))
                    return false;

                if (i == 0)
                    read_direction = current_direction;

                return true;
            }
        };

        const auto & actions = elements_actions[i]->getActions();
        bool ok;
        /// check if it's alias column
        /// currently we only support alias column without any function wrapper
        /// ie: `order by aliased_column` can have this optimization, but `order by function(aliased_column)` can not.
        /// This suits most cases.
        if (context->getSettingsRef().optimize_respect_aliases && aliased_columns.contains(required_sort_description[i].column_name))
        {
            auto column_expr = metadata_snapshot->getColumns().get(required_sort_description[i].column_name).default_desc.expression->clone();
            replaceAliasColumnsInQuery(column_expr, metadata_snapshot->getColumns(), array_join_result_to_source, context);

            auto syntax_analyzer_result = TreeRewriter(context).analyze(column_expr, metadata_snapshot->getColumns().getAll());
            const auto expression_analyzer = ExpressionAnalyzer(column_expr, syntax_analyzer_result, context).getActions(true);
            const auto & alias_actions = expression_analyzer->getActions();

            ok = apply_order_judge(alias_actions, column_expr->getColumnName());
        }
        else
            ok = apply_order_judge(actions, required_sort_description[i].column_name);

        if (ok)
            order_key_prefix_descr.push_back(required_sort_description[i]);
        else
            break;
    }

    if (order_key_prefix_descr.empty())
        return {};
    return std::make_shared<InputOrderInfo>(std::move(order_key_prefix_descr), read_direction);
}

}
