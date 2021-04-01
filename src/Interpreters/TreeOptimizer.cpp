#include <Core/Settings.h>

#include <Interpreters/TreeOptimizer.h>
#include <Interpreters/OptimizeIfChains.h>
#include <Interpreters/OptimizeIfWithConstantConditionVisitor.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>
#include <Interpreters/DuplicateOrderByVisitor.h>
#include <Interpreters/GroupByFunctionKeysVisitor.h>
#include <Interpreters/AggregateFunctionOfGroupByKeysVisitor.h>
#include <Interpreters/RewriteAnyFunctionVisitor.h>
#include <Interpreters/RemoveInjectiveFunctionsVisitor.h>
#include <Interpreters/RedundantFunctionsInOrderByVisitor.h>
#include <Interpreters/RewriteCountVariantsVisitor.h>
#include <Interpreters/MonotonicityCheckVisitor.h>
#include <Interpreters/ConvertStringsToEnumVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Functions/FunctionFactory.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Interpreters/RewriteSumIfFunctionVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

const std::unordered_set<String> possibly_injective_function_names
{
        "dictGet",
        "dictGetString",
        "dictGetUInt8",
        "dictGetUInt16",
        "dictGetUInt32",
        "dictGetUInt64",
        "dictGetInt8",
        "dictGetInt16",
        "dictGetInt32",
        "dictGetInt64",
        "dictGetFloat32",
        "dictGetFloat64",
        "dictGetDate",
        "dictGetDateTime"
};

/** You can not completely remove GROUP BY. Because if there were no aggregate functions, then it turns out that there will be no aggregation.
  * Instead, leave `GROUP BY const`.
  * Next, see deleting the constants in the analyzeAggregation method.
  */
void appendUnusedGroupByColumn(ASTSelectQuery * select_query, const NameSet & source_columns)
{
    /// You must insert a constant that is not the name of the column in the table. Such a case is rare, but it happens.
    UInt64 unused_column = 0;
    String unused_column_name = toString(unused_column);

    while (source_columns.count(unused_column_name))
    {
        ++unused_column;
        unused_column_name = toString(unused_column);
    }

    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::make_shared<ASTExpressionList>());
    select_query->groupBy()->children.emplace_back(std::make_shared<ASTLiteral>(UInt64(unused_column)));
}

/// Eliminates injective function calls and constant expressions from group by statement.
void optimizeGroupBy(ASTSelectQuery * select_query, const NameSet & source_columns, const Context & context)
{
    const FunctionFactory & function_factory = FunctionFactory::instance();

    if (!select_query->groupBy())
    {
        // If there is a HAVING clause without GROUP BY, make sure we have some aggregation happen.
        if (select_query->having())
            appendUnusedGroupByColumn(select_query, source_columns);
        return;
    }

    const auto is_literal = [] (const ASTPtr & ast) -> bool
    {
        return ast->as<ASTLiteral>();
    };

    auto & group_exprs = select_query->groupBy()->children;

    /// removes expression at index idx by making it last one and calling .pop_back()
    const auto remove_expr_at_index = [&group_exprs] (const size_t idx)
    {
        if (idx < group_exprs.size() - 1)
            std::swap(group_exprs[idx], group_exprs.back());

        group_exprs.pop_back();
    };

    /// iterate over each GROUP BY expression, eliminate injective function calls and literals
    for (size_t i = 0; i < group_exprs.size();)
    {
        if (const auto * function = group_exprs[i]->as<ASTFunction>())
        {
            /// assert function is injective
            if (possibly_injective_function_names.count(function->name))
            {
                /// do not handle semantic errors here
                if (function->arguments->children.size() < 2)
                {
                    ++i;
                    continue;
                }

                const auto * dict_name_ast = function->arguments->children[0]->as<ASTLiteral>();
                const auto * attr_name_ast = function->arguments->children[1]->as<ASTLiteral>();
                if (!dict_name_ast || !attr_name_ast)
                {
                    ++i;
                    continue;
                }

                const auto & dict_name = dict_name_ast->value.safeGet<String>();
                const auto & attr_name = attr_name_ast->value.safeGet<String>();

                const auto & dict_ptr = context.getExternalDictionariesLoader().getDictionary(dict_name, context);
                if (!dict_ptr->isInjective(attr_name))
                {
                    ++i;
                    continue;
                }
            }
            else if (!function_factory.get(function->name, context)->isInjective({}))
            {
                ++i;
                continue;
            }

            /// copy shared pointer to args in order to ensure lifetime
            auto args_ast = function->arguments;

            /** remove function call and take a step back to ensure
              * next iteration does not skip not yet processed data
              */
            remove_expr_at_index(i);

            /// copy non-literal arguments
            std::remove_copy_if(
                    std::begin(args_ast->children), std::end(args_ast->children),
                    std::back_inserter(group_exprs), is_literal
            );
        }
        else if (is_literal(group_exprs[i]))
        {
            remove_expr_at_index(i);
        }
        else
        {
            /// if neither a function nor literal - advance to next expression
            ++i;
        }
    }

    if (group_exprs.empty())
        appendUnusedGroupByColumn(select_query, source_columns);
}

struct GroupByKeysInfo
{
    NameSet key_names; ///set of keys' short names
    bool has_function = false;
};

GroupByKeysInfo getGroupByKeysInfo(const ASTs & group_by_keys)
{
    GroupByKeysInfo data;

    /// filling set with short names of keys
    for (const auto & group_key : group_by_keys)
    {
        if (group_key->as<ASTFunction>())
            data.has_function = true;

        data.key_names.insert(group_key->getColumnName());
    }

    return data;
}

///eliminate functions of other GROUP BY keys
void optimizeGroupByFunctionKeys(ASTSelectQuery * select_query)
{
    if (!select_query->groupBy())
        return;

    auto group_by = select_query->groupBy();
    const auto & group_by_keys = group_by->children;

    ASTs modified; ///result

    GroupByKeysInfo group_by_keys_data = getGroupByKeysInfo(group_by_keys);

    if (!group_by_keys_data.has_function)
        return;

    GroupByFunctionKeysVisitor::Data visitor_data{group_by_keys_data.key_names};
    GroupByFunctionKeysVisitor(visitor_data).visit(group_by);

    modified.reserve(group_by_keys.size());

    /// filling the result
    for (const auto & group_key : group_by_keys)
        if (group_by_keys_data.key_names.count(group_key->getColumnName()))
            modified.push_back(group_key);

    /// modifying the input
    group_by->children = modified;
}

/// Eliminates min/max/any-aggregators of functions of GROUP BY keys
void optimizeAggregateFunctionsOfGroupByKeys(ASTSelectQuery * select_query, ASTPtr & node)
{
    if (!select_query->groupBy())
        return;

    const auto & group_by_keys = select_query->groupBy()->children;
    GroupByKeysInfo group_by_keys_data = getGroupByKeysInfo(group_by_keys);

    SelectAggregateFunctionOfGroupByKeysVisitor::Data visitor_data{group_by_keys_data.key_names};
    SelectAggregateFunctionOfGroupByKeysVisitor(visitor_data).visit(node);
}

/// Remove duplicate items from ORDER BY.
void optimizeDuplicatesInOrderBy(const ASTSelectQuery * select_query)
{
    if (!select_query->orderBy())
        return;

    /// Make unique sorting conditions.
    using NameAndLocale = std::pair<String, String>;
    std::set<NameAndLocale> elems_set;

    ASTs & elems = select_query->orderBy()->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        String name = elem->children.front()->getColumnName();
        const auto & order_by_elem = elem->as<ASTOrderByElement &>();

        if (elems_set.emplace(name, order_by_elem.collation ? order_by_elem.collation->getColumnName() : "").second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = std::move(unique_elems);
}

/// Optimize duplicate ORDER BY
void optimizeDuplicateOrderBy(ASTPtr & query, const Context & context)
{
    DuplicateOrderByVisitor::Data order_by_data{context};
    DuplicateOrderByVisitor(order_by_data).visit(query);
}

/// Return simple subselect (without UNIONs or JOINs or SETTINGS) if any
const ASTSelectQuery * getSimpleSubselect(const ASTSelectQuery & select)
{
    if (!select.tables())
        return nullptr;

    const auto & tables = select.tables()->children;
    if (tables.empty() || tables.size() != 1)
        return nullptr;

    const auto & ast_table_expression = tables[0]->as<ASTTablesInSelectQueryElement>()->table_expression;
    if (!ast_table_expression)
        return nullptr;

    const auto & table_expression = ast_table_expression->as<ASTTableExpression>();
    if (!table_expression->subquery)
        return nullptr;

    const auto & subquery = table_expression->subquery->as<ASTSubquery>();
    if (!subquery || subquery->children.size() != 1)
        return nullptr;

    const auto & subselect_union = subquery->children[0]->as<ASTSelectWithUnionQuery>();
    if (!subselect_union || !subselect_union->list_of_selects ||
        subselect_union->list_of_selects->children.size() != 1)
        return nullptr;

    const auto & subselect = subselect_union->list_of_selects->children[0]->as<ASTSelectQuery>();
    if (subselect && subselect->settings())
        return nullptr;

    return subselect;
}

std::unordered_set<String> getDistinctNames(const ASTSelectQuery & select)
{
    if (!select.select() || select.select()->children.empty())
        return {};

    std::unordered_set<String> names;
    std::unordered_set<String> implicit_distinct;

    if (!select.distinct)
    {
        /// SELECT a, b FROM (SELECT DISTINCT a FROM ...)
        if (const ASTSelectQuery * subselect = getSimpleSubselect(select))
            implicit_distinct = getDistinctNames(*subselect);

        if (implicit_distinct.empty())
            return {};
    }

    /// Extract result column names (prefer aliases, ignore table name)
    for (const auto & id : select.select()->children)
    {
        String alias = id->tryGetAlias();

        if (const auto * identifier = id->as<ASTIdentifier>())
        {
            const String & name = identifier->shortName();

            if (select.distinct || implicit_distinct.count(name))
            {
                if (alias.empty())
                    names.insert(name);
                else
                    names.insert(alias);
            }
        }
        else if (select.distinct && !alias.empty())
        {
            /// It's not possible to use getAliasOrColumnName() cause name is context specific (function arguments)
            names.insert(alias);
        }
    }

    /// SELECT a FROM (SELECT DISTINCT a, b FROM ...)
    if (!select.distinct && names.size() != implicit_distinct.size())
        return {};

    return names;
}

/// Remove DISTINCT from query if columns are known as DISTINCT from subquery
void optimizeDuplicateDistinct(ASTSelectQuery & select)
{
    if (!select.select() || select.select()->children.empty())
        return;

    const ASTSelectQuery * subselect = getSimpleSubselect(select);
    if (!subselect)
        return;

    std::unordered_set<String> distinct_names = getDistinctNames(*subselect);
    std::unordered_set<String> selected_names;

    /// Check source column names from select list (ignore aliases and table names)
    for (const auto & id : select.select()->children)
    {
        const auto * identifier = id->as<ASTIdentifier>();
        if (!identifier)
            return;

        String name = identifier->shortName();
        if (!distinct_names.count(name))
            return; /// Not a distinct column, keep DISTINCT for it.

        selected_names.insert(name);
    }

    /// select columns list != distinct columns list
    /// SELECT DISTINCT a FROM (SELECT DISTINCT a, b FROM ...)) -- cannot remove DISTINCT
    if (selected_names.size() != distinct_names.size())
        return;

    select.distinct = false;
}

/// Replace monotonous functions in ORDER BY if they don't participate in GROUP BY expression,
/// has a single argument and not an aggregate functions.
void optimizeMonotonousFunctionsInOrderBy(ASTSelectQuery * select_query, const Context & context,
                                          const TablesWithColumns & tables_with_columns,
                                          const Names & sorting_key_columns)
{
    auto order_by = select_query->orderBy();
    if (!order_by)
        return;

    std::unordered_set<String> group_by_hashes;
    if (auto group_by = select_query->groupBy())
    {
        for (auto & elem : group_by->children)
        {
            auto hash = elem->getTreeHash();
            String key = toString(hash.first) + '_' + toString(hash.second);
            group_by_hashes.insert(key);
        }
    }

    bool is_sorting_key_prefix = true;
    for (size_t i = 0; i < order_by->children.size(); ++i)
    {
        auto * order_by_element = order_by->children[i]->as<ASTOrderByElement>();
        auto & ast_func = order_by_element->children[0];
        if (!ast_func->as<ASTFunction>())
            continue;

        if (i >= sorting_key_columns.size() || ast_func->getColumnName() != sorting_key_columns[i])
            is_sorting_key_prefix = false;

        /// If order by expression matches the sorting key, do not remove
        /// functions to allow execute reading in order of key.
        if (is_sorting_key_prefix)
            continue;

        MonotonicityCheckVisitor::Data data{tables_with_columns, context, group_by_hashes};
        MonotonicityCheckVisitor(data).visit(ast_func);

        if (!data.isRejected())
        {
            ast_func = data.identifier->clone();
            ast_func->setAlias("");
            if (!data.monotonicity.is_positive)
                order_by_element->direction *= -1;
        }
    }
}

/// If ORDER BY has argument x followed by f(x) transforms it to ORDER BY x.
/// Optimize ORDER BY x, y, f(x), g(x, y), f(h(x)), t(f(x), g(x)) into ORDER BY x, y
/// in case if f(), g(), h(), t() are deterministic (in scope of query).
/// Don't optimize ORDER BY f(x), g(x), x even if f(x) is bijection for x or g(x).
void optimizeRedundantFunctionsInOrderBy(const ASTSelectQuery * select_query, const Context & context)
{
    const auto & order_by = select_query->orderBy();
    if (!order_by)
        return;

    std::unordered_set<String> prev_keys;
    ASTs modified;
    modified.reserve(order_by->children.size());

    for (auto & order_by_element : order_by->children)
    {
        /// Order by contains ASTOrderByElement as children and meaning item only as a grand child.
        ASTPtr & name_or_function = order_by_element->children[0];

        if (name_or_function->as<ASTFunction>())
        {
            if (!prev_keys.empty())
            {
                RedundantFunctionsInOrderByVisitor::Data data{prev_keys, context};
                RedundantFunctionsInOrderByVisitor(data).visit(name_or_function);
                if (data.redundant)
                    continue;
            }
        }

        /// @note Leave duplicate keys unchanged. They would be removed in optimizeDuplicatesInOrderBy()
        if (auto * identifier = name_or_function->as<ASTIdentifier>())
            prev_keys.emplace(getIdentifierName(identifier));

        modified.push_back(order_by_element);
    }

    if (modified.size() < order_by->children.size())
        order_by->children = std::move(modified);
}

/// Remove duplicate items from LIMIT BY.
void optimizeLimitBy(const ASTSelectQuery * select_query)
{
    if (!select_query->limitBy())
        return;

    std::set<String> elems_set;

    ASTs & elems = select_query->limitBy()->children;
    ASTs unique_elems;
    unique_elems.reserve(elems.size());

    for (const auto & elem : elems)
    {
        if (elems_set.emplace(elem->getColumnName()).second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = std::move(unique_elems);
}

/// Remove duplicated columns from USING(...).
void optimizeUsing(const ASTSelectQuery * select_query)
{
    if (!select_query->join())
        return;

    const auto * table_join = select_query->join()->table_join->as<ASTTableJoin>();
    if (!(table_join && table_join->using_expression_list))
        return;

    ASTs & expression_list = table_join->using_expression_list->children;
    ASTs uniq_expressions_list;

    std::set<String> expressions_names;

    for (const auto & expression : expression_list)
    {
        auto expression_name = expression->getAliasOrColumnName();
        if (expressions_names.find(expression_name) == expressions_names.end())
        {
            uniq_expressions_list.push_back(expression);
            expressions_names.insert(expression_name);
        }
    }

    if (uniq_expressions_list.size() < expression_list.size())
        expression_list = uniq_expressions_list;
}

void optimizeAggregationFunctions(ASTPtr & query)
{
    /// Move arithmetic operations out of aggregation functions
    ArithmeticOperationsInAgrFuncVisitor::Data data;
    ArithmeticOperationsInAgrFuncVisitor(data).visit(query);
}

void optimizeAnyFunctions(ASTPtr & query)
{
    RewriteAnyFunctionVisitor::Data data = {};
    RewriteAnyFunctionVisitor(data).visit(query);
}

void optimizeSumIfFunctions(ASTPtr & query)
{
    RewriteSumIfFunctionVisitor::Data data = {};
    RewriteSumIfFunctionVisitor(data).visit(query);
}

void optimizeCountConstantAndSumOne(ASTPtr & query)
{
    RewriteCountVariantsVisitor::visit(query);
}


void optimizeInjectiveFunctionsInsideUniq(ASTPtr & query, const Context & context)
{
    RemoveInjectiveFunctionsVisitor::Data data = {context};
    RemoveInjectiveFunctionsVisitor(data).visit(query);
}

void transformIfStringsIntoEnum(ASTPtr & query)
{
    std::unordered_set<String> function_names = {"if", "transform"};
    std::unordered_set<String> used_as_argument;

    FindUsedFunctionsVisitor::Data used_data{function_names, used_as_argument};
    FindUsedFunctionsVisitor(used_data).visit(query);

    ConvertStringsToEnumVisitor::Data convert_data{used_as_argument};
    ConvertStringsToEnumVisitor(convert_data).visit(query);
}

}

void TreeOptimizer::optimizeIf(ASTPtr & query, Aliases & aliases, bool if_chain_to_multiif)
{
    /// Optimize if with constant condition after constants was substituted instead of scalar subqueries.
    OptimizeIfWithConstantConditionVisitor(aliases).visit(query);

    if (if_chain_to_multiif)
        OptimizeIfChainsVisitor().visit(query);
}

void TreeOptimizer::apply(ASTPtr & query, Aliases & aliases, const NameSet & source_columns_set,
                          const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
                          const Context & context, const StorageMetadataPtr & metadata_snapshot,
                          bool & rewrite_subqueries)
{
    const auto & settings = context.getSettingsRef();

    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception("Select analyze for not select asts.", ErrorCodes::LOGICAL_ERROR);

    optimizeIf(query, aliases, settings.optimize_if_chain_to_multiif);

    /// Move arithmetic operations out of aggregation functions
    if (settings.optimize_arithmetic_operations_in_aggregate_functions)
        optimizeAggregationFunctions(query);

    /// Push the predicate expression down to the subqueries.
    rewrite_subqueries = PredicateExpressionsOptimizer(context, tables_with_columns, settings).optimize(*select_query);

    /// GROUP BY injective function elimination.
    optimizeGroupBy(select_query, source_columns_set, context);

    /// GROUP BY functions of other keys elimination.
    if (settings.optimize_group_by_function_keys)
        optimizeGroupByFunctionKeys(select_query);

    /// Move all operations out of any function
    if (settings.optimize_move_functions_out_of_any)
        optimizeAnyFunctions(query);

    if (settings.optimize_normalize_count_variants)
        optimizeCountConstantAndSumOne(query);

    if (settings.optimize_rewrite_sum_if_to_count_if)
        optimizeSumIfFunctions(query);

    /// Remove injective functions inside uniq
    if (settings.optimize_injective_functions_inside_uniq)
        optimizeInjectiveFunctionsInsideUniq(query, context);

    /// Eliminate min/max/any aggregators of functions of GROUP BY keys
    if (settings.optimize_aggregators_of_group_by_keys
        && !select_query->group_by_with_totals
        && !select_query->group_by_with_rollup
        && !select_query->group_by_with_cube)
    {
        optimizeAggregateFunctionsOfGroupByKeys(select_query, query);
    }

    /// Remove duplicate ORDER BY and DISTINCT from subqueries.
    if (settings.optimize_duplicate_order_by_and_distinct)
    {
        optimizeDuplicateOrderBy(query, context);

        /// DISTINCT has special meaning in Distributed query with enabled distributed_group_by_no_merge
        /// TODO: disable Distributed/remote() tables only
        if (!settings.distributed_group_by_no_merge)
            optimizeDuplicateDistinct(*select_query);
    }

    /// Remove functions from ORDER BY if its argument is also in ORDER BY
    if (settings.optimize_redundant_functions_in_order_by)
        optimizeRedundantFunctionsInOrderBy(select_query, context);

    /// Replace monotonous functions with its argument
    if (settings.optimize_monotonous_functions_in_order_by)
        optimizeMonotonousFunctionsInOrderBy(select_query, context, tables_with_columns,
            metadata_snapshot ? metadata_snapshot->getSortingKeyColumns() : Names{});

    /// Remove duplicate items from ORDER BY.
    /// Execute it after all order by optimizations,
    /// because they can produce duplicated columns.
    optimizeDuplicatesInOrderBy(select_query);

    /// If function "if" has String-type arguments, transform them into enum
    if (settings.optimize_if_transform_strings_to_enum)
        transformIfStringsIntoEnum(query);

    /// Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy(select_query);

    /// Remove duplicated columns from USING(...).
    optimizeUsing(select_query);
}

}
