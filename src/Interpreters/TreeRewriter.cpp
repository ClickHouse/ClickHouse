#include <algorithm>
#include <memory>
#include <set>

#include <Core/Settings.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>

#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/ComparisonTupleEliminationVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>
#include <Interpreters/ExpressionActions.h> /// getSmallestColumn()
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/GroupingSetsRewriterVisitor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/RewriteOrderByVisitor.hpp>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/TreeOptimizer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/replaceForPositionalArguments.h>
#include <Interpreters/replaceMissedSubcolumnsInQuery.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTCreateQuery.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <IO/WriteHelpers.h>
#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>
#include <Common/checkStackSize.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageView.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int EMPTY_NESTED_TABLE;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNEXPECTED_EXPRESSION;
}

namespace
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs

void optimizeGroupingSets(ASTPtr & query)
{
    GroupingSetsRewriterVisitor::Data data;
    GroupingSetsRewriterVisitor(data).visit(query);
}

/// Select implementation of a function based on settings.
/// Important that it is done as query rewrite. It means rewritten query
///  will be sent to remote servers during distributed query execution,
///  and on all remote servers, function implementation will be same.
template <char const * func_name>
struct CustomizeFunctionsData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_name;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (Poco::toLower(func.name) == func_name)
        {
            func.name = customized_func_name;
        }
    }
};

char countdistinct[] = "countdistinct";
using CustomizeCountDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countdistinct>>, true>;

char countifdistinct[] = "countifdistinct";
using CustomizeCountIfDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countifdistinct>>, true>;

char countdistinctif[] = "countdistinctif";
using CustomizeCountDistinctIfVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<countdistinctif>>, true>;

char in[] = "in";
using CustomizeInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<in>>, true>;

char notIn[] = "notin";
using CustomizeNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<notIn>>, true>;

char globalIn[] = "globalin";
using CustomizeGlobalInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalIn>>, true>;

char globalNotIn[] = "globalnotin";
using CustomizeGlobalNotInVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsData<globalNotIn>>, true>;

template <char const * func_suffix>
struct CustomizeFunctionsSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (endsWith(Poco::toLower(func.name), func_suffix))
        {
            size_t prefix_len = func.name.length() - strlen(func_suffix);
            func.name = func.name.substr(0, prefix_len) + customized_func_suffix;
        }
    }
};

/// Swap 'if' and 'distinct' suffixes to make execution more optimal.
char ifDistinct[] = "ifdistinct";
using CustomizeIfDistinctVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeFunctionsSuffixData<ifDistinct>>, true>;

/// Used to rewrite all aggregate functions to add -OrNull suffix to them if setting `aggregate_functions_null_for_empty` is set.
struct CustomizeAggregateFunctionsSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        const auto & instance = AggregateFunctionFactory::instance();
        if (instance.isAggregateFunctionName(func.name) && !endsWith(func.name, customized_func_suffix) && !endsWith(func.name, customized_func_suffix + "If"))
        {
            auto properties = instance.tryGetProperties(func.name, func.nulls_action);
            if (properties && !properties->returns_default_when_only_null)
            {
                func.name += customized_func_suffix;
            }
        }
    }
};

// Used to rewrite aggregate functions with -OrNull suffix in some cases, such as sumIfOrNull, we should rewrite to sumOrNullIf
struct CustomizeAggregateFunctionsMoveSuffixData
{
    using TypeToVisit = ASTFunction;

    const String & customized_func_suffix;

    String moveSuffixAhead(const String & name) const
    {
        auto prefix = name.substr(0, name.size() - customized_func_suffix.size());

        auto prefix_size = prefix.size();

        if (endsWith(prefix, "MergeState"))
            return prefix.substr(0, prefix_size - 10) + customized_func_suffix + "MergeState";

        if (endsWith(prefix, "Merge"))
            return prefix.substr(0, prefix_size - 5) + customized_func_suffix + "Merge";

        if (endsWith(prefix, "State"))
            return prefix.substr(0, prefix_size - 5) + customized_func_suffix + "State";

        if (endsWith(prefix, "If"))
            return prefix.substr(0, prefix_size - 2) + customized_func_suffix + "If";

        return name;
    }

    void visit(ASTFunction & func, ASTPtr &) const
    {
        const auto & instance = AggregateFunctionFactory::instance();
        if (instance.isAggregateFunctionName(func.name))
        {
            if (endsWith(func.name, customized_func_suffix))
            {
                auto properties = instance.tryGetProperties(func.name, func.nulls_action);
                if (properties && !properties->returns_default_when_only_null)
                {
                    func.name = moveSuffixAhead(func.name);
                }
            }
        }
    }
};

using CustomizeAggregateFunctionsOrNullVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeAggregateFunctionsSuffixData>, true>;
using CustomizeAggregateFunctionsMoveOrNullVisitor = InDepthNodeVisitor<OneTypeMatcher<CustomizeAggregateFunctionsMoveSuffixData>, true>;

struct ExistsExpressionData
{
    using TypeToVisit = ASTFunction;

    static void visit(ASTFunction & func, ASTPtr)
    {
        bool exists_expression = func.name == "exists"
            && func.arguments && func.arguments->children.size() == 1
            && typeid_cast<const ASTSubquery *>(func.arguments->children[0].get());

        if (!exists_expression)
            return;

        /// EXISTS(subquery) --> 1 IN (SELECT 1 FROM subquery LIMIT 1)

        auto subquery_node = func.arguments->children[0];
        auto table_expression = std::make_shared<ASTTableExpression>();
        table_expression->subquery = std::move(subquery_node);
        table_expression->children.push_back(table_expression->subquery);

        auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();
        tables_in_select_element->table_expression = std::move(table_expression);
        tables_in_select_element->children.push_back(tables_in_select_element->table_expression);

        auto tables_in_select = std::make_shared<ASTTablesInSelectQuery>();
        tables_in_select->children.push_back(std::move(tables_in_select_element));

        auto select_expr_list = std::make_shared<ASTExpressionList>();
        select_expr_list->children.push_back(std::make_shared<ASTLiteral>(1u));

        auto select_query = std::make_shared<ASTSelectQuery>();
        select_query->children.push_back(select_expr_list);

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expr_list);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select);

        ASTPtr limit_length_ast = std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length_ast));

        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
        select_with_union_query->list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->children.push_back(select_with_union_query->list_of_selects);

        auto new_subquery = std::make_shared<ASTSubquery>(std::move(select_with_union_query));

        auto function = makeASTFunction("in", std::make_shared<ASTLiteral>(1u), new_subquery);
        func = *function;
    }
};

using ExistsExpressionVisitor = InDepthNodeVisitor<OneTypeMatcher<ExistsExpressionData>, false>;

struct ReplacePositionalArgumentsData
{
    using TypeToVisit = ASTSelectQuery;

    static void visit(ASTSelectQuery & select_query, ASTPtr &)
    {
        if (select_query.groupBy())
        {
            for (auto & expr : select_query.groupBy()->children)
                replaceForPositionalArguments(expr, &select_query, ASTSelectQuery::Expression::GROUP_BY);
        }
        if (select_query.orderBy())
        {
            for (auto & expr : select_query.orderBy()->children)
            {
                auto & elem = assert_cast<ASTOrderByElement &>(*expr).children.at(0);
                replaceForPositionalArguments(elem, &select_query, ASTSelectQuery::Expression::ORDER_BY);
            }
        }
        if (select_query.limitBy())
        {
            for (auto & expr : select_query.limitBy()->children)
                replaceForPositionalArguments(expr, &select_query, ASTSelectQuery::Expression::LIMIT_BY);
        }
    }
};

using ReplacePositionalArgumentsVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplacePositionalArgumentsData>, false>;

/// Translate qualified names such as db.table.column, table.column, table_alias.column to names' normal form.
/// Expand asterisks and qualified asterisks with column names.
/// There would be columns in normal form & column aliases after translation. Column & column alias would be normalized in QueryNormalizer.
void translateQualifiedNames(ASTPtr & query, const ASTSelectQuery & select_query, const NameSet & source_columns_set,
                             const TablesWithColumns & tables_with_columns)
{
    LogAST log;
    TranslateQualifiedNamesVisitor::Data visitor_data(source_columns_set, tables_with_columns, true/* has_columns */);
    TranslateQualifiedNamesVisitor visitor(visitor_data, log.stream());
    visitor.visit(query);

    /// This may happen after expansion of COLUMNS('regexp').
    if (select_query.select()->children.empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED, "Empty list of columns in SELECT query");
}

bool hasArrayJoin(const ASTPtr & ast)
{
    if (const ASTFunction * function = ast->as<ASTFunction>())
        if (function->name == "arrayJoin")
            return true;

    for (const auto & child : ast->children)
        if (!child->as<ASTSelectQuery>() && hasArrayJoin(child))
            return true;

    return false;
}

/// Keep number of columns for 'GLOBAL IN (SELECT 1 AS a, a)'
void renameDuplicatedColumns(const ASTSelectQuery * select_query)
{
    ASTs & elements = select_query->select()->children;

    std::set<String> all_column_names;
    std::set<String> assigned_column_names;

    for (auto & expr : elements)
        all_column_names.insert(expr->getAliasOrColumnName());

    for (auto & expr : elements)
    {
        auto name = expr->getAliasOrColumnName();

        if (!assigned_column_names.insert(name).second)
        {
            size_t i = 1;
            while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
                ++i;

            name = name + "_" + toString(i);
            expr = expr->clone();   /// Cancels fuse of the same expressions in the tree.
            expr->setAlias(name);

            all_column_names.insert(name);
            assigned_column_names.insert(name);
        }
    }
}

/// Sometimes we have to calculate more columns in SELECT clause than will be returned from query.
/// This is the case when we have DISTINCT or arrayJoin: we require more columns in SELECT even if we need less columns in result.
/// Also we have to remove duplicates in case of GLOBAL subqueries. Their results are placed into tables so duplicates are impossible.
/// Also remove all INTERPOLATE columns which are not in SELECT anymore.
void removeUnneededColumnsFromSelectClause(ASTSelectQuery * select_query, const Names & required_result_columns, bool remove_dups)
{
    ASTs & elements = select_query->select()->children;

    std::map<String, size_t> required_columns_with_duplicate_count;

    if (!required_result_columns.empty())
    {
        /// Some columns may be queried multiple times, like SELECT x, y, y FROM table.
        for (const auto & name : required_result_columns)
        {
            if (remove_dups)
                required_columns_with_duplicate_count[name] = 1;
            else
                ++required_columns_with_duplicate_count[name];
        }
    }
    else if (remove_dups)
    {
        /// Even if we have no requirements there could be duplicates cause of asterisks. SELECT *, t.*
        for (const auto & elem : elements)
            required_columns_with_duplicate_count.emplace(elem->getAliasOrColumnName(), 1);
    }
    else
        return;

    NameSet required_by_interpolate;

    if (select_query->interpolate())
    {
        auto & children = select_query->interpolate()->children;
        if (!children.empty())
        {
            NameToNameSetMap expressions;

            auto interpolate_visitor = [](const ASTPtr ast, NameSet & columns) -> void
            {
                auto interpolate_visitor_impl = [](const ASTPtr node, NameSet & cols, auto self) -> void
                {
                    if (const auto * ident = node->as<ASTIdentifier>())
                        cols.insert(ident->name());
                    else if (const auto * func = node->as<ASTFunction>())
                        for (const auto & elem : func->arguments->children)
                            self(elem, cols, self);
                };
                interpolate_visitor_impl(ast, columns, interpolate_visitor_impl);
            };

            for (const auto & elem : children)
            {
                if (auto * interpolate = elem->as<ASTInterpolateElement>())
                {
                    NameSet needed_columns;
                    interpolate_visitor(interpolate->expr, needed_columns);
                    expressions.emplace(interpolate->column, std::move(needed_columns));
                }
            }

            for (const auto & name : required_result_columns)
                if (const auto it = expressions.find(name); it != expressions.end())
                    required_by_interpolate.insert(it->second.begin(), it->second.end());
        }
    }

    ASTs new_elements;
    new_elements.reserve(elements.size());

    NameSet remove_columns;

    for (const auto & elem : elements)
    {
        String name = elem->getAliasOrColumnName();

        auto it = required_columns_with_duplicate_count.find(name);
        if (required_columns_with_duplicate_count.end() != it && it->second)
        {
            new_elements.push_back(elem);
            --it->second;
        }
        else if (required_by_interpolate.contains(name))
        {
            /// Columns required by interpolate expression are not always in the required_result_columns
            new_elements.push_back(elem);
        }
        else if (select_query->distinct || hasArrayJoin(elem))
        {
            /// ARRAY JOIN cannot be optimized out since it may change number of rows,
            /// so as DISTINCT.
            new_elements.push_back(elem);
        }
        else
        {
            remove_columns.insert(name);

            ASTFunction * func = elem->as<ASTFunction>();

            /// Never remove untuple. It's result column may be in required columns.
            /// It is not easy to analyze untuple here, because types were not calculated yet.
            if (func && func->name == "untuple")
                new_elements.push_back(elem);

            /// removing aggregation can change number of rows, so `count()` result in outer sub-query would be wrong
            if (func && !select_query->groupBy())
            {
                GetAggregatesVisitor::Data data = {};
                GetAggregatesVisitor(data).visit(elem);
                if (!data.aggregates.empty())
                    new_elements.push_back(elem);
            }
        }
    }

    if (select_query->interpolate())
    {
        auto & children = select_query->interpolate()->children;
        if (!children.empty())
        {
            for (auto * it = children.begin(); it != children.end();)
            {
                if (remove_columns.contains((*it)->as<ASTInterpolateElement>()->column))
                    it = select_query->interpolate()->children.erase(it);
                else
                    ++it;
            }

            if (children.empty())
                select_query->setExpression(ASTSelectQuery::Expression::INTERPOLATE, nullptr);
        }
    }

    elements = std::move(new_elements);
}

/// Replacing scalar subqueries with constant values.
void executeScalarSubqueries(
    ASTPtr & query, ContextPtr context, size_t subquery_depth, Scalars & scalars, Scalars & local_scalars, bool only_analyze, bool is_create_parameterized_view)
{
    LogAST log;
    ExecuteScalarSubqueriesVisitor::Data visitor_data{
        WithContext{context}, subquery_depth, scalars,
        local_scalars, only_analyze, is_create_parameterized_view,
        /*replace_only_to_literals=*/ false, /*max_literal_size=*/ std::nullopt};

    ExecuteScalarSubqueriesVisitor(visitor_data, log.stream()).visit(query);
}

void getArrayJoinedColumns(ASTPtr & query, TreeRewriterResult & result, const ASTSelectQuery * select_query,
                           const NamesAndTypesList & source_columns, const NameSet & source_columns_set)
{
    if (!select_query->arrayJoinExpressionList().first)
        return;

    ArrayJoinedColumnsVisitor::Data visitor_data{
        result.aliases, result.array_join_name_to_alias, result.array_join_alias_to_name, result.array_join_result_to_source};
    ArrayJoinedColumnsVisitor(visitor_data).visit(query);

    /// If the result of ARRAY JOIN is not used, it is necessary to ARRAY-JOIN any column,
    /// to get the correct number of rows.
    if (result.array_join_result_to_source.empty())
    {
        if (select_query->arrayJoinExpressionList().first->children.empty())
            throw DB::Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "ARRAY JOIN requires an argument");

        ASTPtr expr = select_query->arrayJoinExpressionList().first->children.at(0);
        String source_name = expr->getColumnName();
        String result_name = expr->getAliasOrColumnName();

        /// This is an array.
        if (!expr->as<ASTIdentifier>() || source_columns_set.contains(source_name))
        {
            result.array_join_result_to_source[result_name] = source_name;
        }
        else /// This is a nested table.
        {
            bool found = false;
            for (const auto & column : source_columns)
            {
                auto split = Nested::splitName(column.name, /*reverse=*/ true);
                if (split.first == source_name && !split.second.empty())
                {
                    result.array_join_result_to_source[Nested::concatenateName(result_name, split.second)] = column.name;
                    found = true;
                    break;
                }
            }
            if (!found)
                throw Exception(ErrorCodes::EMPTY_NESTED_TABLE, "No columns in nested table {}", source_name);
        }
    }
}

void setJoinStrictness(ASTSelectQuery & select_query, JoinStrictness join_default_strictness, bool old_any, std::shared_ptr<TableJoin> & analyzed_join)
{
    const ASTTablesInSelectQueryElement * node = select_query.join();
    if (!node)
        return;

    auto & table_join = const_cast<ASTTablesInSelectQueryElement *>(node)->table_join->as<ASTTableJoin &>();

    if (table_join.strictness == JoinStrictness::Unspecified &&
        table_join.kind != JoinKind::Cross)
    {
        if (join_default_strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::Any;
        else if (join_default_strictness == JoinStrictness::All)
            table_join.strictness = JoinStrictness::All;
        else
            throw Exception(DB::ErrorCodes::EXPECTED_ALL_OR_ANY,
                            "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty");
    }

    if (old_any)
    {
        if (table_join.strictness == JoinStrictness::Any &&
            table_join.kind == JoinKind::Inner)
        {
            table_join.strictness = JoinStrictness::Semi;
            table_join.kind = JoinKind::Left;
        }

        if (table_join.strictness == JoinStrictness::Any)
            table_join.strictness = JoinStrictness::RightAny;
    }
    else
    {
        if (table_join.strictness == JoinStrictness::Any && table_join.kind == JoinKind::Full)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
    }

    analyzed_join->getTableJoin() = table_join;
}

/// Evaluate expression and return boolean value if it can be interpreted as bool.
/// Only UInt8 or NULL are allowed.
/// Returns `false` for 0 or NULL values, `true` for any non-negative value.
std::optional<bool> tryEvaluateConstCondition(ASTPtr expr, ContextPtr context)
{
    if (!expr)
        return {};

    Field eval_res;
    DataTypePtr eval_res_type;
    {
        auto constant_expression_result = tryEvaluateConstantExpression(expr, context);
        if (!constant_expression_result)
            return {};
        std::tie(eval_res, eval_res_type) = std::move(constant_expression_result.value());
    }

    /// UInt8, maybe Nullable, maybe LowCardinality, and NULL are allowed
    eval_res_type = removeNullable(removeLowCardinality(eval_res_type));
    if (auto which = WhichDataType(eval_res_type); !which.isUInt8() && !which.isNothing())
        return {};

    if (eval_res.isNull())
        return false;

    UInt8 res = eval_res.template safeGet<UInt8>();
    return res > 0;
}

bool tryJoinOnConst(TableJoin & analyzed_join, const ASTPtr & on_expression, ContextPtr context)
{
    if (!analyzed_join.isEnabledAlgorithm(JoinAlgorithm::HASH))
        return false;

    if (analyzed_join.strictness() == JoinStrictness::Asof)
        return false;

    if (analyzed_join.isSpecialStorage())
        return false;

    if (auto eval_const_res = tryEvaluateConstCondition(on_expression, context))
    {
        if (eval_const_res.value())
        {
            /// JOIN ON 1 == 1
            LOG_DEBUG(getLogger("TreeRewriter"), "Join on constant executed as cross join");
            analyzed_join.resetToCross();
        }
        else
        {
            /// JOIN ON 1 != 1
            LOG_DEBUG(getLogger("TreeRewriter"), "Join on constant executed as empty join");
            analyzed_join.resetKeys();
        }
        return true;
    }
    return false;
}

/// Find the columns that are obtained by JOIN.
void collectJoinedColumns(TableJoin & analyzed_join, ASTTableJoin & table_join,
                          const TablesWithColumns & tables, const Aliases & aliases, ContextPtr context)
{
    assert(tables.size() >= 2);

    if (table_join.using_expression_list)
    {
        const auto & keys = table_join.using_expression_list->as<ASTExpressionList &>();

        analyzed_join.addDisjunct();
        for (const auto & key : keys.children)
            analyzed_join.addUsingKey(key);
    }
    else if (table_join.on_expression)
    {
        bool join_on_const_ok = tryJoinOnConst(analyzed_join, table_join.on_expression, context);
        if (join_on_const_ok)
            return;

        bool is_asof = (table_join.strictness == JoinStrictness::Asof);

        CollectJoinOnKeysVisitor::Data data{analyzed_join, tables[0], tables[1], aliases, is_asof};
        if (auto * or_func = table_join.on_expression->as<ASTFunction>(); or_func && or_func->name == "or")
        {
            for (auto & disjunct : or_func->arguments->children)
            {
                analyzed_join.addDisjunct();
                CollectJoinOnKeysVisitor(data).visit(disjunct);
            }
            assert(analyzed_join.getClauses().size() == or_func->arguments->children.size());
        }
        else
        {
            analyzed_join.addDisjunct();
            CollectJoinOnKeysVisitor(data).visit(table_join.on_expression);
            assert(analyzed_join.oneDisjunct());
        }

        auto check_keys_empty = [] (auto e) { return e.key_names_left.empty(); };
        bool any_keys_empty = std::any_of(analyzed_join.getClauses().begin(), analyzed_join.getClauses().end(), check_keys_empty);

        if (any_keys_empty)
            throw DB::Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                                "Cannot get JOIN keys from JOIN ON section: '{}', found keys: {}",
                                queryToString(table_join.on_expression), TableJoin::formatClauses(analyzed_join.getClauses()));

        if (is_asof)
        {
            if (!analyzed_join.oneDisjunct())
                throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join doesn't support multiple ORs for keys in JOIN ON section");
            data.asofToJoinKeys();
        }

        if (!analyzed_join.oneDisjunct() && !analyzed_join.isEnabledAlgorithm(JoinAlgorithm::HASH))
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `hash` join supports multiple ORs for keys in JOIN ON section");
    }
}

std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(const ASTPtr & expr, ASTExpressionList & into)
{
    checkStackSize();

    if (expr->as<ASTIdentifier>())
    {
        into.children.push_back(expr);
        return {false, 1};
    }

    auto * function = expr->as<ASTFunction>();

    if (!function)
        return {false, 0};

    if (AggregateUtils::isAggregateFunction(*function))
        return {true, 0};

    UInt64 pushed_children = 0;
    bool has_aggregate = false;

    for (const auto & child : function->arguments->children)
    {
        auto [child_has_aggregate, child_pushed_children] = recursivelyCollectMaxOrdinaryExpressions(child, into);
        has_aggregate |= child_has_aggregate;
        pushed_children += child_pushed_children;
    }

    /// The current function is not aggregate function and there is no aggregate function in its arguments,
    /// so use the current function to replace its arguments
    if (!has_aggregate)
    {
        for (UInt64 i = 0; i < pushed_children; i++)
            into.children.pop_back();

        into.children.push_back(expr);
        pushed_children = 1;
    }

    return {has_aggregate, pushed_children};
}

/** Expand GROUP BY ALL by extracting all the SELECT-ed expressions that are not aggregate functions.
  *
  * For a special case that if there is a function having both aggregate functions and other fields as its arguments,
  * the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.
  *
  * Example:
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY ALL
  * will expand as
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY substring(a, 4, 2), substring(a, 1, 2)
  */
void expandGroupByAll(ASTSelectQuery * select_query)
{
    auto group_expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & expr : select_query->select()->children)
        recursivelyCollectMaxOrdinaryExpressions(expr, *group_expression_list);

    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_expression_list);
}

void expandOrderByAll(ASTSelectQuery * select_query, [[maybe_unused]] const TablesWithColumns & tables_with_columns)
{
    auto * all_elem = select_query->orderBy()->children[0]->as<ASTOrderByElement>();
    if (!all_elem)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not order by asts.");

    auto order_expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & expr : select_query->select()->children)
    {
        /// Detect and reject ambiguous statements:
        /// E.g. for a table with columns "all", "a", "b":
        /// - SELECT all, a, b ORDER BY all;        -- should we sort by all columns in SELECT or by column "all"?
        /// - SELECT a, b AS all ORDER BY all;      -- like before but "all" as alias
        /// - SELECT func(...) AS all ORDER BY all; -- like before but "all" as function
        /// - SELECT a, b ORDER BY all;             -- tricky in other way: does the user want to sort by columns in SELECT clause or by not SELECTed column "all"?

        static const String all = "all";
        if (auto * identifier = expr->as<ASTIdentifier>(); identifier != nullptr)
            if (boost::iequals(identifier->name(), all) || boost::iequals(identifier->alias, all))
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");

        if (auto * function = expr->as<ASTFunction>(); function != nullptr)
            if (boost::iequals(function->alias, all))
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");

        for (const auto & table_with_columns : tables_with_columns)
        {
            const auto & columns = table_with_columns.columns;
            if (columns.containsCaseInsensitive(all))
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");
        }

        auto elem = std::make_shared<ASTOrderByElement>();
        elem->direction = all_elem->direction;
        elem->nulls_direction = all_elem->nulls_direction;
        elem->nulls_direction_was_explicitly_specified = all_elem->nulls_direction_was_explicitly_specified;
        elem->children.push_back(expr);
        order_expression_list->children.push_back(elem);
    }

    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_expression_list);
}

ASTs getAggregates(ASTPtr & query, const ASTSelectQuery & select_query)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query.where())
        assertNoAggregates(select_query.where(), "in WHERE");
    if (select_query.prewhere())
        assertNoAggregates(select_query.prewhere(), "in PREWHERE");

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    /// There can not be other aggregate functions within the aggregate functions.
    for (const ASTPtr & ast : data.aggregates)
    {
        const ASTFunction & node = typeid_cast<const ASTFunction &>(*ast);
        if (node.arguments)
        {
            for (auto & arg : node.arguments->children)
            {
                assertNoAggregates(arg, "inside another aggregate function");
                // We also can't have window functions inside aggregate functions,
                // because the window functions are calculated later.
                assertNoWindows(arg, "inside an aggregate function");
            }
        }
    }
    return data.aggregates;
}

ASTs getWindowFunctions(ASTPtr & query, const ASTSelectQuery & select_query)
{
    /// There can not be window functions inside the WHERE, PREWHERE and HAVING
    if (select_query.having())
        assertNoWindows(select_query.having(), "in HAVING");
    if (select_query.where())
        assertNoWindows(select_query.where(), "in WHERE");
    if (select_query.prewhere())
        assertNoWindows(select_query.prewhere(), "in PREWHERE");
    if (select_query.window())
        assertNoWindows(select_query.window(), "in WINDOW");

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    /// Window functions cannot be inside aggregates or other window functions.
    /// Aggregate functions can be inside window functions because they are
    /// calculated earlier.
    for (const ASTPtr & ast : data.window_functions)
    {
        const ASTFunction & node = typeid_cast<const ASTFunction &>(*ast);

        if (node.arguments)
            for (auto & arg : node.arguments->children)
                assertNoWindows(arg, "inside another window function");

        if (node.window_definition)
            assertNoWindows(node.window_definition, "inside window definition");
    }

    return data.window_functions;
}

class MarkTupleLiteralsAsLegacyData
{
public:
    struct Data
    {
    };

    static void visitLiteral(ASTLiteral & literal, ASTPtr &)
    {
        if (literal.value.getType() == Field::Types::Tuple)
            literal.use_legacy_column_name_of_tuple = true;
    }
    static void visitFunction(ASTFunction & func, ASTPtr &ast)
    {
        if (func.name == "tuple" && func.arguments && !func.arguments->children.empty())
        {
            // re-write tuple() function as literal
            if (auto literal = func.toLiteral())
            {
                ast = literal;
                visitLiteral(*typeid_cast<ASTLiteral *>(ast.get()), ast);
            }
        }
    }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * identifier = typeid_cast<ASTFunction *>(ast.get()))
            visitFunction(*identifier, ast);
        if (auto * identifier = typeid_cast<ASTLiteral *>(ast.get()))
            visitLiteral(*identifier, ast);
    }

    static bool needChildVisit(const ASTPtr & /*parent*/, const ASTPtr & /*child*/)
    {
        return true;
    }
};

using MarkTupleLiteralsAsLegacyVisitor = InDepthNodeVisitor<MarkTupleLiteralsAsLegacyData, true>;

void markTupleLiteralsAsLegacy(ASTPtr & query)
{
    MarkTupleLiteralsAsLegacyVisitor::Data data;
    MarkTupleLiteralsAsLegacyVisitor(data).visit(query);
}

/// Rewrite _shard_num -> shardNum() AS _shard_num
struct RewriteShardNum
{
    struct Data
    {
    };

    static bool needChildVisit(const ASTPtr & parent, const ASTPtr & /*child*/)
    {
        /// ON section should not be rewritten.
        return typeid_cast<ASTTableJoin  *>(parent.get()) == nullptr;
    }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * identifier = typeid_cast<ASTIdentifier *>(ast.get()))
            visit(*identifier, ast);
    }

    static void visit(ASTIdentifier & identifier, ASTPtr & ast)
    {
        if (identifier.shortName() != "_shard_num")
            return;

        String alias = identifier.tryGetAlias();
        if (alias.empty())
            alias = "_shard_num";
        ast = makeASTFunction("shardNum");
        ast->setAlias(alias);
    }
};
using RewriteShardNumVisitor = InDepthNodeVisitor<RewriteShardNum, true>;

}

TreeRewriterResult::TreeRewriterResult(
    const NamesAndTypesList & source_columns_,
    ConstStoragePtr storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    bool add_special)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , source_columns(source_columns_)
{
    collectSourceColumns(add_special);
    is_remote_storage = storage && storage->isRemote();
}

/// Add columns from storage to source_columns list. Deduplicate resulted list.
/// Special columns are non physical columns, for example ALIAS
void TreeRewriterResult::collectSourceColumns(bool add_special)
{
    if (storage)
    {
        auto options = GetColumnsOptions(add_special ? GetColumnsOptions::All : GetColumnsOptions::AllPhysical);
        options.withExtendedObjects();
        options.withSubcolumns(storage->supportsSubcolumns());

        auto columns_from_storage = storage_snapshot->getColumns(options);

        if (source_columns.empty())
            source_columns.swap(columns_from_storage);
        else
            source_columns.insert(source_columns.end(), columns_from_storage.begin(), columns_from_storage.end());

        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        source_columns_ordinary = metadata_snapshot->getColumns().getOrdinary();
    }

    source_columns_set = removeDuplicateColumns(source_columns);
}


/// Calculate which columns are required to execute the expression.
/// Then, delete all other columns from the list of available columns.
/// After execution, columns will only contain the list of columns needed to read from the table.
bool TreeRewriterResult::collectUsedColumns(const ASTPtr & query, bool is_select, bool no_throw)
{
    /// We calculate required_source_columns with source_columns modifications and swap them on exit
    required_source_columns = source_columns;

    RequiredSourceColumnsVisitor::Data columns_context;
    RequiredSourceColumnsVisitor(columns_context).visit(query);

    NameSet source_column_names;
    for (const auto & column : source_columns)
        source_column_names.insert(column.name);

    NameSet required = columns_context.requiredColumns();
    if (columns_context.has_table_join)
    {
        NameSet available_columns;
        for (const auto & name : source_columns)
            available_columns.insert(name.name);

        /// Add columns obtained by JOIN (if needed).
        for (const auto & joined_column : analyzed_join->columnsFromJoinedTable())
        {
            const auto & name = joined_column.name;
            if (available_columns.contains(name))
                continue;

            if (required.contains(name))
            {
                /// Optimization: do not add columns needed only in JOIN ON section.
                if (columns_context.nameInclusion(name) > analyzed_join->rightKeyInclusion(name))
                    analyzed_join->addJoinedColumn(joined_column);

                required.erase(name);
            }
        }
    }

    NameSet array_join_sources;
    if (columns_context.has_array_join)
    {
        /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
        for (const auto & result_source : array_join_result_to_source)
            array_join_sources.insert(result_source.second);

        for (const auto & column_name_type : source_columns)
            if (array_join_sources.contains(column_name_type.name))
                required.insert(column_name_type.name);
    }

    /// Figure out if we're able to use the trivial count optimization.
    has_explicit_columns = !required.empty();
    if (is_select && !has_explicit_columns)
    {
        optimize_trivial_count = !columns_context.has_array_join;

        /// You need to read at least one column to find the number of rows.
        /// We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
        /// Because it is the column that is cheapest to read.
        struct ColumnSizeTuple
        {
            size_t compressed_size;
            size_t type_size;
            size_t uncompressed_size;
            String name;

            bool operator<(const ColumnSizeTuple & that) const
            {
                return std::tie(compressed_size, type_size, uncompressed_size)
                    < std::tie(that.compressed_size, that.type_size, that.uncompressed_size);
            }
        };

        std::vector<ColumnSizeTuple> columns;
        if (storage)
        {
            auto column_sizes = storage->getColumnSizes();
            for (auto & source_column : source_columns)
            {
                auto c = column_sizes.find(source_column.name);
                if (c == column_sizes.end())
                    continue;
                size_t type_size = source_column.type->haveMaximumSizeOfValue() ? source_column.type->getMaximumSizeOfValueInMemory() : 100;
                columns.emplace_back(ColumnSizeTuple{c->second.data_compressed, type_size, c->second.data_uncompressed, source_column.name});
            }
        }

        if (!columns.empty())
            required.insert(std::min_element(columns.begin(), columns.end())->name);
        else if (!source_columns.empty())
            /// If we have no information about columns sizes, choose a column of minimum size of its data type.
            required.insert(ExpressionActions::getSmallestColumn(source_columns).name);
    }
    else if (is_select && storage_snapshot && !columns_context.has_array_join)
    {
        const auto & partition_desc = storage_snapshot->metadata->getPartitionKey();
        if (partition_desc.expression)
        {
            auto partition_columns = partition_desc.expression->getRequiredColumns();
            NameSet partition_columns_set(partition_columns.begin(), partition_columns.end());

            const auto & parititon_virtuals = MergeTreeData::virtuals_useful_for_filter;
            partition_columns_set.insert(parititon_virtuals.begin(), parititon_virtuals.end());

            optimize_trivial_count = true;
            for (const auto & required_column : required)
            {
                if (!partition_columns_set.contains(required_column))
                {
                    optimize_trivial_count = false;
                    break;
                }
            }
        }
    }

    NameSet unknown_required_source_columns = required;

    for (auto it = source_columns.begin(); it != source_columns.end();)
    {
        const String & column_name = it->name;
        unknown_required_source_columns.erase(column_name);

        if (!required.contains(column_name))
            it = source_columns.erase(it);
        else
            ++it;
    }

    has_virtual_shard_num = false;
    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage_snapshot)
    {
        const auto & virtuals = storage_snapshot->virtual_columns;
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            if (auto column = virtuals->tryGet(*it))
            {
                source_columns.push_back(*column);
                it = unknown_required_source_columns.erase(it);
            }
            else
            {
                ++it;
            }
        }

        has_virtual_shard_num
            = is_remote_storage && storage->isVirtualColumn("_shard_num", storage_snapshot->metadata) && virtuals->has("_shard_num");
    }

    /// Collect missed object subcolumns
    if (!unknown_required_source_columns.empty())
    {
        for (const NameAndTypePair & pair : source_columns_ordinary)
        {
            for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
            {
                size_t object_pos = it->find('.');
                if (object_pos != std::string::npos)
                {
                    String object_name = it->substr(0, object_pos);
                    if (pair.name == object_name && pair.type->getTypeId() == TypeIndex::Object)
                    {
                        const auto * object_type = typeid_cast<const DataTypeObject *>(pair.type.get());
                        if (object_type->getSchemaFormat() == "json" && object_type->hasNullableSubcolumns())
                        {
                            missed_subcolumns.insert(*it);
                            it = unknown_required_source_columns.erase(it);
                            continue;
                        }
                    }
                }
                ++it;
            }
        }
    }

    /// Check for dynamic subcolumns in unknown required columns.
    if (!unknown_required_source_columns.empty())
    {
        for (const NameAndTypePair & pair : source_columns_ordinary)
        {
            if (!pair.type->hasDynamicSubcolumns())
                continue;

            for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
            {
                auto [column_name, dynamic_subcolumn_name] = Nested::splitName(*it);

                if (column_name == pair.name)
                {
                    if (auto dynamic_subcolumn_type = pair.type->tryGetSubcolumnType(dynamic_subcolumn_name))
                    {
                        source_columns.emplace_back(*it, dynamic_subcolumn_type);
                        it = unknown_required_source_columns.erase(it);
                        continue;
                    }
                }

                ++it;
            }
        }
    }

    if (!unknown_required_source_columns.empty())
    {
        constexpr auto format_string = "Missing columns: {} while processing query: '{}', required columns:{}{}";
        WriteBufferFromOwnString ss;
        ss << "Missing columns:";
        for (const auto & name : unknown_required_source_columns)
            ss << " '" << name << "'";
        ss << " while processing query: '" << queryToString(query) << "'";

        ss << ", required columns:";
        for (const auto & name : columns_context.requiredColumns())
            ss << " '" << name << "'";

        if (storage)
        {
            std::vector<String> hint_name{};
            std::set<String> helper_hint_name{};
            for (const auto & name : columns_context.requiredColumns())
            {
                auto hints = storage->getHints(name);
                for (const auto & hint : hints)
                {
                    // We want to preserve the ordering of the hints
                    // (as they are ordered by Levenshtein distance)
                    auto [_, inserted] = helper_hint_name.insert(hint);
                    if (inserted)
                        hint_name.push_back(hint);
                }
            }

            if (!hint_name.empty())
            {
                ss << ", maybe you meant: ";
                ss << toStringWithFinalSeparator(hint_name, " or ");
            }
        }
        else
        {
            if (!source_column_names.empty())
                for (const auto & name : columns_context.requiredColumns())
                    ss << " '" << name << "'";
            else
                ss << ", no source columns";
        }

        if (columns_context.has_table_join)
        {
            ss << ", joined columns:";
            for (const auto & column : analyzed_join->columnsFromJoinedTable())
                ss << " '" << column.name << "'";
        }

        if (!array_join_sources.empty())
        {
            ss << ", arrayJoin columns:";
            for (const auto & name : array_join_sources)
                ss << " '" << name << "'";
        }

        if (no_throw)
            return false;
        throw Exception(PreformattedMessage{ss.str(), format_string, std::vector<std::string>{}}, ErrorCodes::UNKNOWN_IDENTIFIER);
    }

    required_source_columns.swap(source_columns);
    for (const auto & column : required_source_columns)
    {
        source_column_names.insert(column.name);
    }
    return true;
}

NameSet TreeRewriterResult::getArrayJoinSourceNameSet() const
{
    NameSet forbidden_columns;
    for (const auto & elem : array_join_result_to_source)
        forbidden_columns.insert(elem.first);
    return forbidden_columns;
}

TreeRewriterResultPtr TreeRewriter::analyzeSelect(
    ASTPtr & query,
    TreeRewriterResult && result,
    const SelectQueryOptions & select_options,
    const TablesWithColumns & tables_with_columns,
    const Names & required_result_columns,
    std::shared_ptr<TableJoin> table_join) const
{
    auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not select asts.");

    size_t subquery_depth = select_options.subquery_depth;
    bool remove_duplicates = select_options.remove_duplicates;

    const auto & settings = getContext()->getSettingsRef();

    const NameSet & source_columns_set = result.source_columns_set;

    if (table_join)
    {
        result.analyzed_join = table_join;
        result.analyzed_join->resetCollected();
    }
    else /// TODO: remove. For now ExpressionAnalyzer expects some not empty object here
        result.analyzed_join = std::make_shared<TableJoin>();

    if (remove_duplicates)
        renameDuplicatedColumns(select_query);

    /// Perform it before analyzing JOINs, because it may change number of columns with names unique and break some logic inside JOINs
    if (settings.optimize_normalize_count_variants)
        TreeOptimizer::optimizeCountConstantAndSumOne(query, getContext());

    if (tables_with_columns.size() > 1)
    {
        const auto & right_table = tables_with_columns[1];
        auto columns_from_joined_table = right_table.columns;
        /// query can use materialized or aliased columns from right joined table,
        /// we want to request it for right table
        columns_from_joined_table.insert(columns_from_joined_table.end(), right_table.hidden_columns.begin(), right_table.hidden_columns.end());
        result.analyzed_join->setColumnsFromJoinedTable(std::move(columns_from_joined_table), source_columns_set, right_table.table.getQualifiedNamePrefix());
    }

    translateQualifiedNames(query, *select_query, source_columns_set, tables_with_columns);

    /// Optimizes logical expressions.
    LogicalExpressionsOptimizer(select_query, tables_with_columns, settings.optimize_min_equality_disjunction_chain_length.value).perform();

    NameSet all_source_columns_set = source_columns_set;
    if (table_join)
    {
        for (const auto & [name, _] : table_join->columnsFromJoinedTable())
            all_source_columns_set.insert(name);
    }

    normalize(query, result.aliases, all_source_columns_set, select_options.ignore_alias, settings, /* allow_self_aliases = */ true, getContext(), select_options.is_create_parameterized_view);

    // expand GROUP BY ALL
    if (select_query->group_by_all)
        expandGroupByAll(select_query);

    // expand ORDER BY ALL
    if (settings.enable_order_by_all && select_query->order_by_all)
        expandOrderByAll(select_query, tables_with_columns);

    /// Remove unneeded columns according to 'required_result_columns'.
    /// Leave all selected columns in case of DISTINCT; columns that contain arrayJoin function inside.
    /// Must be after 'normalizeTree' (after expanding aliases, for aliases not get lost)
    ///  and before 'executeScalarSubqueries', 'analyzeAggregation', etc. to avoid excessive calculations.
    removeUnneededColumnsFromSelectClause(select_query, required_result_columns, remove_duplicates);

    /// Executing scalar subqueries - replacing them with constant values.
    Scalars scalars;
    Scalars local_scalars;
    executeScalarSubqueries(
        query,
        getContext(),
        subquery_depth,
        scalars,
        local_scalars,
        select_options.only_analyze,
        select_options.is_create_parameterized_view);

    /// Save scalar sub queries's results in the query context
    /// Note that we are only saving scalars and not local_scalars since the latter can't be safely shared across contexts
    if (!select_options.only_analyze && getContext()->hasQueryContext())
    {
        for (const auto & it : scalars)
            getContext()->getQueryContext()->addScalar(it.first, it.second);
    }

    if (settings.legacy_column_name_of_tuple_literal)
        markTupleLiteralsAsLegacy(query);

    /// Push the predicate expression down to subqueries. The optimization should be applied to both initial and secondary queries.
    result.rewrite_subqueries = PredicateExpressionsOptimizer(getContext(), tables_with_columns, settings).optimize(*select_query);

     /// Only apply AST optimization for initial queries.
    const bool ast_optimizations_allowed =
        getContext()->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY
        && !select_options.ignore_ast_optimizations;

    bool optimize_multiif_to_if = ast_optimizations_allowed && settings.optimize_multiif_to_if;
    TreeOptimizer::optimizeIf(query, result.aliases, settings.optimize_if_chain_to_multiif, optimize_multiif_to_if);

    if (ast_optimizations_allowed)
        TreeOptimizer::apply(query, result, tables_with_columns, getContext());

    /// array_join_alias_to_name, array_join_result_to_source.
    getArrayJoinedColumns(query, result, select_query, result.source_columns, source_columns_set);

    setJoinStrictness(
        *select_query, settings.join_default_strictness, settings.any_join_distinct_right_table_keys, result.analyzed_join);

    auto * table_join_ast = select_query->join() ? select_query->join()->table_join->as<ASTTableJoin>() : nullptr;
    if (table_join_ast && tables_with_columns.size() >= 2)
        collectJoinedColumns(*result.analyzed_join, *table_join_ast, tables_with_columns, result.aliases, getContext());

    result.aggregates = getAggregates(query, *select_query);
    result.window_function_asts = getWindowFunctions(query, *select_query);
    result.expressions_with_window_function = getExpressionsWithWindowFunctions(query);

    result.collectUsedColumns(query, true);

    if (!result.missed_subcolumns.empty())
    {
        for (const String & column_name : result.missed_subcolumns)
            replaceMissedSubcolumnsInQuery(query, column_name);
        result.missed_subcolumns.clear();
    }

    result.required_source_columns_before_expanding_alias_columns = result.required_source_columns.getNames();

    /// rewrite filters for select query, must go after getArrayJoinedColumns
    bool is_initiator = getContext()->getClientInfo().distributed_depth == 0;
    if (settings.optimize_respect_aliases && result.storage_snapshot && is_initiator)
    {
        std::unordered_set<IAST *> excluded_nodes;
        {
            /// Do not replace ALIASed columns in JOIN ON/USING sections
            if (table_join_ast && table_join_ast->on_expression)
                excluded_nodes.insert(table_join_ast->on_expression.get());
            if (table_join_ast && table_join_ast->using_expression_list)
                excluded_nodes.insert(table_join_ast->using_expression_list.get());
        }

        bool is_changed = replaceAliasColumnsInQuery(query, result.storage_snapshot->metadata->getColumns(),
                                                     result.array_join_result_to_source, getContext(), excluded_nodes);

        /// If query is changed, we need to redo some work to correct name resolution.
        if (is_changed)
        {
            /// We should re-apply the optimization, because an expression substituted from alias column might be a function of a group key.
            if (ast_optimizations_allowed && settings.optimize_group_by_function_keys)
                TreeOptimizer::optimizeGroupByFunctionKeys(select_query);

            result.aggregates = getAggregates(query, *select_query);
            result.window_function_asts = getWindowFunctions(query, *select_query);
            result.expressions_with_window_function = getExpressionsWithWindowFunctions(query);
            result.collectUsedColumns(query, true);
        }
    }

    /// Rewrite _shard_num to shardNum()
    if (result.has_virtual_shard_num)
    {
        RewriteShardNumVisitor::Data data_rewrite_shard_num;
        RewriteShardNumVisitor(data_rewrite_shard_num).visit(query);
    }

    result.ast_join = select_query->join();

    if (result.optimize_trivial_count)
        result.optimize_trivial_count = settings.optimize_trivial_count_query &&
            !select_query->groupBy() && !select_query->having() &&
            !select_query->sampleSize() && !select_query->sampleOffset() && !select_query->final() &&
            (tables_with_columns.size() < 2 || isLeft(result.analyzed_join->kind()));

    // remove outer braces in order by
    RewriteOrderByVisitor::Data data;
    RewriteOrderByVisitor(data).visit(query);

    return std::make_shared<const TreeRewriterResult>(result);
}

TreeRewriterResultPtr TreeRewriter::analyze(
    ASTPtr & query,
    const NamesAndTypesList & source_columns,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    bool allow_aggregations,
    bool allow_self_aliases,
    bool execute_scalar_subqueries,
    bool is_create_parameterized_view) const
{
    if (query->as<ASTSelectQuery>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not select analyze for select asts.");

    const auto & settings = getContext()->getSettingsRef();

    TreeRewriterResult result(source_columns, storage, storage_snapshot, false);

    normalize(query, result.aliases, result.source_columns_set, false, settings, allow_self_aliases, getContext(), is_create_parameterized_view);

    /// Executing scalar subqueries. Column defaults could be a scalar subquery.
    Scalars scalars;
    Scalars local_scalars;
    executeScalarSubqueries(query, getContext(), 0, scalars, local_scalars, !execute_scalar_subqueries, is_create_parameterized_view);

    /// Save scalar sub queries's results in the query context
    /// Note that we are only saving scalars and not local_scalars since the latter can't be safely shared across contexts
    if (execute_scalar_subqueries && getContext()->hasQueryContext())
    {
        for (const auto & it : scalars)
            getContext()->getQueryContext()->addScalar(it.first, it.second);
    }

    if (settings.legacy_column_name_of_tuple_literal)
        markTupleLiteralsAsLegacy(query);

    TreeOptimizer::optimizeIf(query, result.aliases, settings.optimize_if_chain_to_multiif, false);

    if (allow_aggregations)
    {
        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor(data).visit(query);

        /// There can not be other aggregate functions within the aggregate functions.
        for (const ASTPtr & node : data.aggregates)
            for (auto & arg : typeid_cast<const ASTFunction &>(*node).arguments->children)
                assertNoAggregates(arg, "inside another aggregate function");
        result.aggregates = data.aggregates;
    }
    else
        assertNoAggregates(query, "in wrong place");

    bool is_ok = result.collectUsedColumns(query, false, no_throw);
    if (!is_ok)
        return {};

    if (!result.missed_subcolumns.empty())
    {
        for (const String & column_name : result.missed_subcolumns)
            replaceMissedSubcolumnsInQuery(query, column_name);
        result.missed_subcolumns.clear();
    }

    return std::make_shared<const TreeRewriterResult>(result);
}

void TreeRewriter::normalize(
    ASTPtr & query, Aliases & aliases, const NameSet & source_columns_set, bool ignore_alias, const Settings & settings, bool allow_self_aliases, ContextPtr context_, bool is_create_parameterized_view)
{
    if (!UserDefinedSQLFunctionFactory::instance().empty())
        UserDefinedSQLFunctionVisitor::visit(query);

    CustomizeCountDistinctVisitor::Data data_count_distinct{settings.count_distinct_implementation};
    CustomizeCountDistinctVisitor(data_count_distinct).visit(query);

    CustomizeCountIfDistinctVisitor::Data data_count_if_distinct{settings.count_distinct_implementation.toString() + "If"};
    CustomizeCountIfDistinctVisitor(data_count_if_distinct).visit(query);

    CustomizeIfDistinctVisitor::Data data_distinct_if{"DistinctIf"};
    CustomizeIfDistinctVisitor(data_distinct_if).visit(query);

    if (settings.rewrite_count_distinct_if_with_count_distinct_implementation)
    {
        CustomizeCountDistinctIfVisitor::Data data_count_distinct_if{settings.count_distinct_implementation.toString() + "If"};
        CustomizeCountDistinctIfVisitor(data_count_distinct_if).visit(query);
    }

    ExistsExpressionVisitor::Data exists;
    ExistsExpressionVisitor(exists).visit(query);

    if (context_->getSettingsRef().enable_positional_arguments)
    {
        ReplacePositionalArgumentsVisitor::Data data_replace_positional_arguments;
        ReplacePositionalArgumentsVisitor(data_replace_positional_arguments).visit(query);
    }

    if (settings.transform_null_in)
    {
        CustomizeInVisitor::Data data_null_in{"nullIn"};
        CustomizeInVisitor(data_null_in).visit(query);

        CustomizeNotInVisitor::Data data_not_null_in{"notNullIn"};
        CustomizeNotInVisitor(data_not_null_in).visit(query);

        CustomizeGlobalInVisitor::Data data_global_null_in{"globalNullIn"};
        CustomizeGlobalInVisitor(data_global_null_in).visit(query);

        CustomizeGlobalNotInVisitor::Data data_global_not_null_in{"globalNotNullIn"};
        CustomizeGlobalNotInVisitor(data_global_not_null_in).visit(query);
    }

    /// Rewrite all aggregate functions to add -OrNull suffix to them
    if (settings.aggregate_functions_null_for_empty)
    {
        CustomizeAggregateFunctionsOrNullVisitor::Data data_or_null{"OrNull"};
        CustomizeAggregateFunctionsOrNullVisitor(data_or_null).visit(query);
    }

    /// Move -OrNull suffix ahead, this should execute after add -OrNull suffix
    CustomizeAggregateFunctionsMoveOrNullVisitor::Data data_or_null{"OrNull"};
    CustomizeAggregateFunctionsMoveOrNullVisitor(data_or_null).visit(query);

    /// Creates a dictionary `aliases`: alias -> ASTPtr
    QueryAliasesVisitor(aliases).visit(query);

    /// Mark table ASTIdentifiers with not a column marker
    MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
    MarkTableIdentifiersVisitor(identifiers_data).visit(query);

    /// Rewrite function names to their canonical ones.
    /// Notice: function name normalization is disabled when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    if (context_->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY && settings.normalize_function_names)
        FunctionNameNormalizer::visit(query.get());

    if (settings.optimize_move_to_prewhere)
    {
        /// Required for PREWHERE
        ComparisonTupleEliminationVisitor::Data data_comparison_tuple_elimination;
        ComparisonTupleEliminationVisitor(data_comparison_tuple_elimination).visit(query);
    }

    /// Common subexpression elimination. Rewrite rules.
    QueryNormalizer::Data normalizer_data(aliases, source_columns_set, ignore_alias, settings, allow_self_aliases, is_create_parameterized_view);
    QueryNormalizer(normalizer_data).visit(query);

    optimizeGroupingSets(query);
}

}
