#include <Core/Settings.h>

#include <Interpreters/TreeOptimizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/OptimizeIfChains.h>
#include <Interpreters/OptimizeIfWithConstantConditionVisitor.h>
#include <Interpreters/WhereConstraintsOptimizer.h>
#include <Interpreters/SubstituteColumnOptimizer.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ArithmeticOperationsInAgrFuncOptimize.h>
#include <Interpreters/DuplicateOrderByVisitor.h>
#include <Interpreters/GroupByFunctionKeysVisitor.h>
#include <Interpreters/AggregateFunctionOfGroupByKeysVisitor.h>
#include <Interpreters/RemoveInjectiveFunctionsVisitor.h>
#include <Interpreters/FunctionMaskingArgumentCheckVisitor.h>
#include <Interpreters/RedundantFunctionsInOrderByVisitor.h>
#include <Interpreters/RewriteCountVariantsVisitor.h>
#include <Interpreters/ConvertStringsToEnumVisitor.h>
#include <Interpreters/ConvertFunctionOrLikeVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/GatherFunctionQuantileVisitor.h>
#include <Interpreters/RewriteArrayExistsFunctionVisitor.h>
#include <Interpreters/RewriteSumFunctionWithSumAndCountVisitor.h>
#include <Interpreters/OptimizeDateOrDateTimeConverterWithPreimageVisitor.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Storages/IStorage.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_hyperscan;
    extern const SettingsBool convert_query_to_cnf;
    extern const SettingsBool enable_positional_arguments;
    extern const SettingsUInt64 max_hyperscan_regexp_length;
    extern const SettingsUInt64 max_hyperscan_regexp_total_length;
    extern const SettingsBool optimize_aggregators_of_group_by_keys;
    extern const SettingsBool optimize_append_index;
    extern const SettingsBool optimize_arithmetic_operations_in_aggregate_functions;
    extern const SettingsBool optimize_group_by_function_keys;
    extern const SettingsBool optimize_if_transform_strings_to_enum;
    extern const SettingsBool optimize_injective_functions_inside_uniq;
    extern const SettingsBool optimize_normalize_count_variants;
    extern const SettingsBool optimize_substitute_columns;
    extern const SettingsBool optimize_time_filter_with_preimage;
    extern const SettingsBool optimize_using_constraints;
    extern const SettingsBool optimize_redundant_functions_in_order_by;
    extern const SettingsBool optimize_rewrite_array_exists_to_has;
    extern const SettingsBool optimize_or_like_chain;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
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
void appendUnusedGroupByColumn(ASTSelectQuery * select_query)
{
    /// Since ASTLiteral is different from ASTIdentifier, so we can use a special constant String Literal for this,
    /// and do not need to worry about it conflict with the name of the column in the table.
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::make_shared<ASTExpressionList>());
    select_query->groupBy()->children.emplace_back(std::make_shared<ASTLiteral>("__unused_group_by_column"));
}

/// Eliminates injective function calls and constant expressions from group by statement.
void optimizeGroupBy(ASTSelectQuery * select_query, ContextPtr context)
{
    const FunctionFactory & function_factory = FunctionFactory::instance();

    if (!select_query->groupBy())
        return;

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

    const auto & settings = context->getSettingsRef();

    /// iterate over each GROUP BY expression, eliminate injective function calls and literals
    for (size_t i = 0; i < group_exprs.size();)
    {
        if (const auto * function = group_exprs[i]->as<ASTFunction>())
        {
            /// assert function is injective
            if (possibly_injective_function_names.contains(function->name))
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

                const auto & dict_ptr = context->getExternalDictionariesLoader().getDictionary(dict_name, context);
                if (!dict_ptr->isInjective(attr_name))
                {
                    ++i;
                    continue;
                }
            }
            else
            {
                FunctionOverloadResolverPtr function_builder = UserDefinedExecutableFunctionFactory::instance().tryGet(function->name, context); /// NOLINT(readability-static-accessed-through-instance)

                if (!function_builder)
                    function_builder = function_factory.get(function->name, context);

                if (!function_builder->isInjective({}))
                {
                    ++i;
                    continue;
                }
            }
            /// don't optimize functions that shadow any of it's arguments, e.g.:
            /// SELECT toString(dummy) as dummy FROM system.one GROUP BY dummy;
            if (!function->alias.empty())
            {
                FunctionMaskingArgumentCheckVisitor::Data data{.alias=function->alias};
                FunctionMaskingArgumentCheckVisitor(data).visit(function->arguments);

                if (data.is_rejected)
                {
                    ++i;
                    continue;
                }
            }

            /// copy shared pointer to args in order to ensure lifetime
            auto args_ast = function->arguments;
            /// Replace function call in 'group_exprs' with non-literal arguments.
            const auto & erase_position = group_exprs.begin() + i;
            group_exprs.erase(erase_position);
            const auto & insert_position = group_exprs.begin() + i;
            (void)std::remove_copy_if(
                std::begin(args_ast->children), std::end(args_ast->children),
                std::inserter(group_exprs, insert_position), is_literal);
        }
        else if (is_literal(group_exprs[i]))
        {
            bool keep_position = false;
            if (settings[Setting::enable_positional_arguments])
            {
                const auto & value = group_exprs[i]->as<ASTLiteral>()->value;
                if (value.getType() == Field::Types::UInt64)
                {
                    auto pos = value.safeGet<UInt64>();
                    if (pos > 0 && pos <= select_query->select()->children.size())
                        keep_position = true;
                }
            }

            if (keep_position)
                ++i;
            else
                remove_expr_at_index(i);
        }
        else
        {
            /// if neither a function nor literal - advance to next expression
            ++i;
        }
    }

    if (group_exprs.empty())
        appendUnusedGroupByColumn(select_query);
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
        /// for grouping sets case
        if (group_key->as<ASTExpressionList>())
        {
            const auto express_list_ast = group_key->as<const ASTExpressionList &>();
            for (const auto & group_elem : express_list_ast.children)
            {
                data.key_names.insert(group_elem->getColumnName());
            }
        }
        else
        {
            if (group_key->as<ASTFunction>())
                data.has_function = true;

            data.key_names.insert(group_key->getColumnName());
        }
    }

    return data;
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

        if (order_by_elem.with_fill /// Always keep elements WITH FILL as they affects other.
            || elems_set.emplace(name, order_by_elem.getCollation() ? order_by_elem.getCollation()->getColumnName() : "").second)
            unique_elems.emplace_back(elem);
    }

    if (unique_elems.size() < elems.size())
        elems = std::move(unique_elems);
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

            if (select.distinct || implicit_distinct.contains(name))
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

/// If ORDER BY has argument x followed by f(x) transforms it to ORDER BY x.
/// Optimize ORDER BY x, y, f(x), g(x, y), f(h(x)), t(f(x), g(x)) into ORDER BY x, y
/// in case if f(), g(), h(), t() are deterministic (in scope of query).
/// Don't optimize ORDER BY f(x), g(x), x even if f(x) is bijection for x or g(x).
void optimizeRedundantFunctionsInOrderBy(const ASTSelectQuery * select_query, ContextPtr context)
{
    const auto & order_by = select_query->orderBy();
    if (!order_by)
        return;

    for (const auto & child : order_by->children)
    {
        auto * order_by_element = child->as<ASTOrderByElement>();

        if (!order_by_element || order_by_element->children.empty())
            throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE, "Bad ORDER BY expression AST");

        if (order_by_element->with_fill)
            return;
    }

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

/// Use constraints to get rid of useless parts of query
void optimizeWithConstraints(ASTSelectQuery * select_query,
                             Aliases & /*aliases*/,
                             const NameSet & /*source_columns_set*/,
                             const std::vector<TableWithColumnNamesAndTypes> & /*tables_with_columns*/,
                             const StorageMetadataPtr & metadata_snapshot,
                             const bool optimize_append_index)
{
    WhereConstraintsOptimizer(select_query, metadata_snapshot, optimize_append_index).perform();
}

void optimizeSubstituteColumn(ASTSelectQuery * select_query,
                              Aliases & /*aliases*/,
                              const NameSet & /*source_columns_set*/,
                              const std::vector<TableWithColumnNamesAndTypes> & /*tables_with_columns*/,
                              const StorageMetadataPtr & metadata_snapshot,
                              const ConstStoragePtr & storage)
{
    SubstituteColumnOptimizer(select_query, metadata_snapshot, storage).perform();
}

/// Transform WHERE to CNF for more convenient optimization.
bool convertQueryToCNF(ASTSelectQuery * select_query)
{
    if (select_query->where())
    {
        auto cnf_form = TreeCNFConverter::tryConvertToCNF(select_query->where());
        if (!cnf_form)
            return false;

        cnf_form->pushNotInFunctions();
        select_query->refWhere() = TreeCNFConverter::fromCNF(*cnf_form);
        return true;
    }

    return false;
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

void optimizeArrayExistsFunctions(ASTPtr & query)
{
    RewriteArrayExistsFunctionVisitor::Data data = {};
    RewriteArrayExistsFunctionVisitor(data).visit(query);
}

void optimizeMultiIfToIf(ASTPtr & query)
{
    OptimizeMultiIfToIfVisitor::Data data;
    OptimizeMultiIfToIfVisitor(data).visit(query);
}

void optimizeInjectiveFunctionsInsideUniq(ASTPtr & query, ContextPtr context)
{
    RemoveInjectiveFunctionsVisitor::Data data(context);
    RemoveInjectiveFunctionsVisitor(data).visit(query);
}

void optimizeDateFilters(ASTSelectQuery * select_query, const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns, ContextPtr context)
{
    /// Predicates in HAVING clause has been moved to WHERE clause.
    if (select_query->where())
    {
        OptimizeDateOrDateTimeConverterWithPreimageVisitor::Data data{tables_with_columns, context};
        OptimizeDateOrDateTimeConverterWithPreimageVisitor(data).visit(select_query->refWhere());
    }
    if (select_query->prewhere())
    {
        OptimizeDateOrDateTimeConverterWithPreimageVisitor::Data data{tables_with_columns, context};
        OptimizeDateOrDateTimeConverterWithPreimageVisitor(data).visit(select_query->refPrewhere());
    }
}

void rewriteSumFunctionWithSumAndCount(ASTPtr & query, const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns)
{
    RewriteSumFunctionWithSumAndCountVisitor::Data data = {tables_with_columns};
    RewriteSumFunctionWithSumAndCountVisitor(data).visit(query);
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

void optimizeOrLikeChain(ASTPtr & query)
{
    ConvertFunctionOrLikeVisitor::Data data = {};
    ConvertFunctionOrLikeVisitor(data).visit(query);
}

}

void TreeOptimizer::optimizeIf(ASTPtr & query, Aliases & aliases, bool if_chain_to_multiif, bool multiif_to_if)
{
    if (multiif_to_if)
        optimizeMultiIfToIf(query);

    /// Optimize if with constant condition after constants was substituted instead of scalar subqueries.
    OptimizeIfWithConstantConditionVisitorData visitor_data(aliases);
    OptimizeIfWithConstantConditionVisitor(visitor_data).visit(query);

    if (if_chain_to_multiif)
        OptimizeIfChainsVisitor().visit(query);
}

void TreeOptimizer::optimizeCountConstantAndSumOne(ASTPtr & query, ContextPtr context)
{
    RewriteCountVariantsVisitor(context).visit(query);
}

///eliminate functions of other GROUP BY keys
void TreeOptimizer::optimizeGroupByFunctionKeys(ASTSelectQuery * select_query)
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
        if (group_by_keys_data.key_names.contains(group_key->getColumnName()))
            modified.push_back(group_key);

    /// modifying the input
    group_by->children = modified;
}

void TreeOptimizer::apply(ASTPtr & query, TreeRewriterResult & result,
                          const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();

    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not select asts.");

    /// Move arithmetic operations out of aggregation functions
    if (settings[Setting::optimize_arithmetic_operations_in_aggregate_functions])
        optimizeAggregationFunctions(query);

    bool converted_to_cnf = false;
    if (settings[Setting::convert_query_to_cnf])
        converted_to_cnf = convertQueryToCNF(select_query);

    if (converted_to_cnf && settings[Setting::optimize_using_constraints] && result.storage_snapshot)
    {
        optimizeWithConstraints(
            select_query,
            result.aliases,
            result.source_columns_set,
            tables_with_columns,
            result.storage_snapshot->metadata,
            settings[Setting::optimize_append_index]);

        if (settings[Setting::optimize_substitute_columns])
            optimizeSubstituteColumn(select_query, result.aliases, result.source_columns_set,
                tables_with_columns, result.storage_snapshot->metadata, result.storage);
    }

    /// Rewrite sum(column +/- literal) function with sum(column) +/- literal * count(column).
    if (settings[Setting::optimize_arithmetic_operations_in_aggregate_functions])
        rewriteSumFunctionWithSumAndCount(query, tables_with_columns);

    /// Rewrite date filters to avoid the calls of converters such as toYear, toYYYYMM, etc.
    if (settings[Setting::optimize_time_filter_with_preimage])
        optimizeDateFilters(select_query, tables_with_columns, context);

    /// GROUP BY injective function elimination.
    optimizeGroupBy(select_query, context);

    /// GROUP BY functions of other keys elimination.
    if (settings[Setting::optimize_group_by_function_keys])
        optimizeGroupByFunctionKeys(select_query);

    if (settings[Setting::optimize_normalize_count_variants])
        optimizeCountConstantAndSumOne(query, context);

    if (settings[Setting::optimize_rewrite_array_exists_to_has])
        optimizeArrayExistsFunctions(query);

    /// Remove injective functions inside uniq
    if (settings[Setting::optimize_injective_functions_inside_uniq])
        optimizeInjectiveFunctionsInsideUniq(query, context);

    /// Eliminate min/max/any aggregators of functions of GROUP BY keys
    if (settings[Setting::optimize_aggregators_of_group_by_keys]
        && !select_query->group_by_with_totals
        && !select_query->group_by_with_rollup
        && !select_query->group_by_with_cube)
        optimizeAggregateFunctionsOfGroupByKeys(select_query, query);

    /// Remove functions from ORDER BY if its argument is also in ORDER BY
    if (settings[Setting::optimize_redundant_functions_in_order_by])
        optimizeRedundantFunctionsInOrderBy(select_query, context);

    /// Remove duplicate items from ORDER BY.
    /// Execute it after all order by optimizations,
    /// because they can produce duplicated columns.
    optimizeDuplicatesInOrderBy(select_query);

    /// If function "if" has String-type arguments, transform them into enum
    if (settings[Setting::optimize_if_transform_strings_to_enum])
        transformIfStringsIntoEnum(query);

    /// Remove duplicated elements from LIMIT BY clause.
    optimizeLimitBy(select_query);

    /// Remove duplicated columns from USING(...).
    optimizeUsing(select_query);

    if (settings[Setting::optimize_or_like_chain] && settings[Setting::allow_hyperscan] && settings[Setting::max_hyperscan_regexp_length] == 0
        && settings[Setting::max_hyperscan_regexp_total_length] == 0)
    {
        optimizeOrLikeChain(query);
    }
}

}
