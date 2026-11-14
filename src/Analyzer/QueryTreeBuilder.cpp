#include <Interpreters/applyColumnsTransformer.h>
#include <Analyzer/QueryTreeBuilder.h>

#include <unordered_set>

#include <Common/FieldVisitorToString.h>
#include <Common/SettingSource.h>
#include <Common/quoteString.h>

#include <DataTypes/FieldToDataType.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTWithAlias.h>
#include <Core/Streaming/CursorTree.h>

#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTStreamSettings.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/UnionNode.h>

#include <AggregateFunctions/AggregateFunctionGroupConcat.h>

#include <Core/Settings.h>

#include <Databases/IDatabase.h>

#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_shuffle_query;
    extern const SettingsBool any_join_distinct_right_table_keys;
    extern const SettingsJoinStrictness join_default_strictness;
    extern const SettingsBool enable_order_by_all;
    extern const SettingsBool enable_unaligned_array_join;
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsString implicit_table_at_top_level;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}


namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int EXPECTED_ALL_OR_ANY;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_QUERY_PARAMETER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// An `Identifier` query parameter that was never substituted: an empty name part, with the parameter as a child.
void throwIfQueryParameter(const ASTIdentifier & identifier)
{
    if (!identifier.isParam())
        return;

    const auto & parameter = static_cast<const IAST &>(identifier).children.front()->as<ASTQueryParameter &>();
    throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER, "Query parameter {} was not set", backQuote(parameter.name));
}

/// Column names from the `name (col1, col2, ...)` alias list of a CTE/table expression
/// Duplicates are rejected, they would collapse columns during identifier resolution
Names getColumnAliasNames(const ASTPtr & column_aliases)
{
    const auto & column_aliases_list = column_aliases->as<const ASTExpressionList &>();

    Names result;
    result.reserve(column_aliases_list.children.size());

    NameSet unique_aliases;
    for (const auto & column_alias : column_aliases_list.children)
    {
        const auto & alias_name = column_alias->as<const ASTIdentifier &>().name();
        if (!unique_aliases.insert(alias_name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate column alias '{}' in column alias list", alias_name);
        result.push_back(alias_name);
    }

    return result;
}

/// `unnest(e1, ..., en) [WITH ORDINALITY] [[AS] alias[(c1, ...)]]` in `FROM`.
struct UnnestTableExpression
{
    ASTs arguments;
    bool with_ordinality = false;
    String table_alias;
    /// One name per argument, then the ordinality column.
    Names column_names;
};

std::optional<UnnestTableExpression> tryGetUnnestTableExpression(const ASTTableExpression & table_expression)
{
    if (!table_expression.table_function)
        return {};

    const auto & function = table_expression.table_function->as<const ASTFunction &>();
    if (Poco::toLower(function.name) != "unnest")
        return {};

    UnnestTableExpression result;
    if (function.arguments)
        result.arguments = function.arguments->children;

    if (result.arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function {} requires at least one argument",
            function.formatForErrorMessage());

    for (const auto & argument : result.arguments)
        if (!dynamic_cast<const ASTWithAlias *>(argument.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function {} expects expressions as arguments, got {}",
                function.formatForErrorMessage(), argument->formatForErrorMessage());

    result.with_ordinality = table_expression.with_ordinality;
    result.table_alias = function.tryGetAlias();
    if (result.table_alias.empty())
        result.table_alias = "unnest";

    size_t arguments_size = result.arguments.size();
    size_t columns_size = arguments_size + result.with_ordinality;

    Names listed_names;
    if (table_expression.column_aliases)
        listed_names = getColumnAliasNames(table_expression.column_aliases);

    if (listed_names.size() > columns_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function {} has {} columns, but {} column aliases are specified",
            function.formatForErrorMessage(), columns_size, listed_names.size());

    NameSet unique_names;
    for (size_t i = 0; i < columns_size; ++i)
    {
        String name;
        if (i < listed_names.size())
            name = listed_names[i];
        else if (i == arguments_size)
            name = "ordinality";
        else if (arguments_size == 1)
            name = result.table_alias;
        else
            name = "unnest" + std::to_string(i + 1);

        if (!unique_names.insert(name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function {} has multiple columns with name {}",
                function.formatForErrorMessage(), backQuote(name));

        result.column_names.push_back(std::move(name));
    }

    return result;
}

/// The joins after which `unnest(...)` is evaluated for each row of the preceding tables, as in PostgreSQL.
bool isLateralUnnestJoin(const ASTTableJoin & table_join)
{
    bool is_lateral_kind = table_join.kind == JoinKind::Comma || table_join.kind == JoinKind::Cross
        || table_join.kind == JoinKind::Inner || table_join.kind == JoinKind::Left;

    return is_lateral_kind
        && (table_join.strictness == JoinStrictness::Unspecified || table_join.strictness == JoinStrictness::All)
        && table_join.locality == JoinLocality::Unspecified
        && !table_join.is_natural
        && !table_join.using_expression_list;
}

/// Every identifier part and alias in `ast`, including subqueries and lambda parameters.
void collectNames(const ASTPtr & ast, NameSet & names)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        names.insert(identifier->name_parts.begin(), identifier->name_parts.end());

    if (auto alias = ast->tryGetAlias(); !alias.empty())
        names.insert(std::move(alias));

    for (const auto & child : ast->children)
        collectNames(child, names);
}

String makeFreshName(String name, NameSet & used_names)
{
    while (used_names.contains(name))
        name = "_" + name;

    used_names.insert(name);
    return name;
}

ASTPtr makeSelectWithUnionQuery(ASTPtr select_expression_list, ASTPtr from_select_with_union_query)
{
    auto select_query = make_intrusive<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    if (from_select_with_union_query)
    {
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->subquery = make_intrusive<ASTSubquery>(std::move(from_select_with_union_query));
        table_expression->children.push_back(table_expression->subquery);

        auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
        table_element->table_expression = table_expression;
        table_element->children.push_back(table_element->table_expression);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(table_element));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    }

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union_query->children.push_back(std::move(list_of_selects));
    select_with_union_query->list_of_selects = select_with_union_query->children.back();

    return select_with_union_query;
}

/// `length(e1)`; for several arrays the longest one counts.
ASTPtr buildUnnestLength(const ASTs & arguments)
{
    if (arguments.size() == 1)
        return makeASTFunction("length", arguments.front()->clone());

    auto greatest = makeASTFunction("greatest");
    for (const auto & argument : arguments)
        greatest->arguments->children.push_back(makeASTFunction("length", argument->clone()));
    return greatest;
}

/// `range(1, length(e1) + 1)`.
ASTPtr buildUnnestOrdinalityArray(const ASTs & arguments)
{
    return makeASTFunction("range",
        make_intrusive<ASTLiteral>(UInt64(1)),
        makeASTOperator("plus", buildUnnestLength(arguments), make_intrusive<ASTLiteral>(UInt64(1))));
}

/// With `enable_unaligned_array_join`, several arrays are padded to the longest one.
ASTs buildUnnestArrays(const ASTs & arguments, bool pad_unaligned)
{
    ASTs arrays;
    for (const auto & argument : arguments)
    {
        if (pad_unaligned && arguments.size() > 1)
            arrays.push_back(makeASTFunction("arrayResize", argument->clone(), buildUnnestLength(arguments)));
        else
            arrays.push_back(argument->clone());
    }
    return arrays;
}

/// Standalone `unnest(...)`: `SELECT arrayJoin(e1) AS c1`, or one `arrayJoin` of the zipped arrays whose
/// tuple elements are the columns.
ASTPtr buildUnnestSubquery(const UnnestTableExpression & unnest, bool pad_unaligned)
{
    if (unnest.arguments.size() == 1 && !unnest.with_ordinality)
    {
        auto select_expression_list = make_intrusive<ASTExpressionList>();
        select_expression_list->children.push_back(
            setAlias(makeASTFunction("arrayJoin", unnest.arguments.front()->clone()), unnest.column_names.front()));
        return makeSelectWithUnionQuery(std::move(select_expression_list), nullptr);
    }

    NameSet used_names(unnest.column_names.begin(), unnest.column_names.end());
    for (const auto & argument : unnest.arguments)
        collectNames(argument, used_names);
    auto element_name = makeFreshName("__unnest_element", used_names);

    auto zipped_arrays = makeASTFunction("arrayZip");
    for (auto & array : buildUnnestArrays(unnest.arguments, pad_unaligned))
        zipped_arrays->arguments->children.push_back(std::move(array));
    if (unnest.with_ordinality)
        zipped_arrays->arguments->children.push_back(buildUnnestOrdinalityArray(unnest.arguments));

    auto inner_select_expression_list = make_intrusive<ASTExpressionList>();
    inner_select_expression_list->children.push_back(setAlias(makeASTFunction("arrayJoin", std::move(zipped_arrays)), element_name));

    auto select_expression_list = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < unnest.column_names.size(); ++i)
    {
        auto element = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(element_name), make_intrusive<ASTLiteral>(UInt64(i + 1)));
        select_expression_list->children.push_back(setAlias(std::move(element), unnest.column_names[i]));
    }

    return makeSelectWithUnionQuery(std::move(select_expression_list), makeSelectWithUnionQuery(std::move(inner_select_expression_list), nullptr));
}

/// `ON 1` and `ON true` keep every element.
bool isTrivialUnnestJoinCondition(const ASTPtr & on_expression)
{
    if (!on_expression)
        return true;

    const auto * literal = on_expression->as<ASTLiteral>();
    return literal && (literal->value.getType() == Field::Types::UInt64 || literal->value.getType() == Field::Types::Bool)
        && literal->value.safeGet<UInt64>() == 1;
}

bool isLambdaWithParameter(const ASTPtr & ast, const String & name)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !(function->isLambdaFunction() || isASTLambdaFunction(*function)))
        return false;

    const auto & lambda_arguments = function->arguments->children.at(0)->as<ASTFunction &>();
    if (lambda_arguments.arguments)
        for (const auto & lambda_argument : lambda_arguments.arguments->children)
            if (const auto * lambda_argument_identifier = lambda_argument->as<ASTIdentifier>();
                lambda_argument_identifier && lambda_argument_identifier->name() == name)
                return true;

    return false;
}

/// Replaces `u.c[.rest]`, where `u` is `table_alias` and `c` has a binder, with `binder[.rest]`, except inside
/// subqueries and inside a lambda with a parameter named `u`.
void replaceUnnestQualifiedColumns(ASTPtr & ast, const String & table_alias, const std::unordered_map<String, String> & column_to_binder)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        const auto & name_parts = identifier->name_parts;
        if (name_parts.size() < 2 || name_parts[0] != table_alias)
            return;

        auto it = column_to_binder.find(name_parts[1]);
        if (it == column_to_binder.end())
            return;

        std::vector<String> replacement_name_parts{it->second};
        replacement_name_parts.insert(replacement_name_parts.end(), name_parts.begin() + 2, name_parts.end());
        auto replacement = make_intrusive<ASTIdentifier>(std::move(replacement_name_parts));
        replacement->setAlias(identifier->tryGetAlias());
        ast = std::move(replacement);
        return;
    }

    if (ast->as<ASTSubquery>() || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>())
        return;

    if (isLambdaWithParameter(ast, table_alias))
        return;

    for (auto & child : ast->children)
        replaceUnnestQualifiedColumns(child, table_alias, column_to_binder);
}

/** `t, unnest(e1, ..., en) AS u(c1, ...)` is `t ARRAY JOIN e1 AS c1, ...`. A non-trivial `ON c` filters every array
  * with the per-row mask `arrayMap((c1, ..., ck, Q1, ..., Qk) -> c, A1, ..., Ak, A1, ..., Ak)`, where `u.ci` in `c` is
  * replaced by the fresh name `Qi`, so that no lambda in `c` can capture it.
  */
ASTPtr buildUnnestArrayJoinExpressions(const UnnestTableExpression & unnest, const ASTPtr & on_expression, bool pad_unaligned)
{
    bool has_condition = !isTrivialUnnestJoinCondition(on_expression);

    /// Without a condition this is a plain `ARRAY JOIN`, which pads by itself.
    ASTs arrays = buildUnnestArrays(unnest.arguments, pad_unaligned && has_condition);
    if (unnest.with_ordinality)
        arrays.push_back(buildUnnestOrdinalityArray(unnest.arguments));

    if (has_condition)
    {
        NameSet used_names(unnest.column_names.begin(), unnest.column_names.end());
        collectNames(on_expression, used_names);

        Strings mask_parameters = unnest.column_names;
        std::unordered_map<String, String> column_to_binder;
        for (size_t i = 0; i < unnest.column_names.size(); ++i)
        {
            auto binder = makeFreshName("__unnest_column_" + std::to_string(i + 1), used_names);
            column_to_binder.emplace(unnest.column_names[i], binder);
            mask_parameters.push_back(std::move(binder));
        }

        auto condition = on_expression->clone();
        replaceUnnestQualifiedColumns(condition, unnest.table_alias, column_to_binder);

        auto mask = makeASTFunction("arrayMap", makeASTLambda(mask_parameters, std::move(condition)));
        for (size_t i = 0; i < 2; ++i)
            for (const auto & array : arrays)
                mask->arguments->children.push_back(array->clone());

        for (auto & array : arrays)
            array = makeASTFunction("arrayFilter",
                makeASTLambda({"__unnest_element", "__unnest_match"}, make_intrusive<ASTIdentifier>("__unnest_match")),
                std::move(array),
                mask->clone());
    }

    auto array_join_expressions = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < arrays.size(); ++i)
        array_join_expressions->children.push_back(setAlias(std::move(arrays[i]), unnest.column_names[i]));

    return array_join_expressions;
}

class QueryTreeBuilder
{
public:

    QueryTreeNodePtr buildQueryTreeNode(ASTPtr query_, ContextPtr context_);

    std::shared_ptr<TableFunctionNode> buildTableFunction(const ASTPtr & table_function, const ContextPtr & context) const;

private:

    struct CommonTableExpressionData
    {
        std::string_view cte_name;
        bool is_materialized = false;
    };

    QueryTreeNodePtr buildSelectOrUnionExpression(
        const ASTPtr & select_or_union_query,
        bool is_subquery,
        const CommonTableExpressionData & cte_data,
        const ASTPtr & aliases,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectWithUnionExpression(
        const ASTPtr & select_with_union_query,
        bool is_subquery,
        const CommonTableExpressionData & cte_data,
        const ASTPtr & aliases,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectIntersectExceptQuery(
        const ASTPtr & select_intersect_except_query,
        bool is_subquery,
        const CommonTableExpressionData & cte_data,
        const ASTPtr & aliases,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSelectExpression(
        const ASTPtr & select_query,
        bool is_subquery,
        const CommonTableExpressionData & cte_data,
        const ASTPtr & aliases,
        const ContextPtr & context) const;

    QueryTreeNodePtr buildSortList(const ASTPtr & order_by_expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildInterpolateList(const ASTPtr & interpolate_expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildWindowList(const ASTPtr & window_definition_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildExpressionList(const ASTPtr & expression_list, const ContextPtr & context) const;

    QueryTreeNodePtr buildExpression(const ASTPtr & expression, const ContextPtr & context) const;

    QueryTreeNodePtr buildWindow(const ASTPtr & window_definition, const ContextPtr & context) const;

    QueryTreeNodePtr buildJoinTree(bool is_subquery, const ASTSelectQuery & select_query, const ContextPtr & context) const;

    ColumnTransformersNodes buildColumnTransformers(const ASTPtr & matcher_expression, const ContextPtr & context) const;

    QueryTreeNodePtr setSecondArgumentAsParameter(const ASTFunction * function, const ContextPtr & context) const;
};

QueryTreeNodePtr QueryTreeBuilder::buildQueryTreeNode(ASTPtr query_, ContextPtr context_)
{
    auto query = query_->clone();
    QueryTreeNodePtr query_tree_node;

    if (query->as<ASTSelectWithUnionQuery>() ||
        query->as<ASTSelectIntersectExceptQuery>() ||
        query->as<ASTSelectQuery>())
        query_tree_node = buildSelectOrUnionExpression(query, false /*is_subquery*/, {} /*cte_name*/, nullptr /*aliases*/, context_);
    else if (query->as<ASTExpressionList>())
        query_tree_node = buildExpressionList(query, context_);
    else
        query_tree_node = buildExpression(query, context_);

    return query_tree_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectOrUnionExpression(
    const ASTPtr & select_or_union_query,
    bool is_subquery,
    const CommonTableExpressionData & cte_data,
    const ASTPtr & aliases,
    const ContextPtr & context) const
{
    QueryTreeNodePtr query_node;

    if (select_or_union_query->as<ASTSelectWithUnionQuery>())
        query_node = buildSelectWithUnionExpression(select_or_union_query, is_subquery, cte_data, aliases, context);
    else if (select_or_union_query->as<ASTSelectIntersectExceptQuery>())
        query_node = buildSelectIntersectExceptQuery(select_or_union_query, is_subquery, cte_data, aliases, context);
    else if (select_or_union_query->as<ASTSelectQuery>())
        query_node = buildSelectExpression(select_or_union_query, is_subquery, cte_data, aliases, context);
    else
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "SELECT or UNION query {} is not supported",
                        select_or_union_query->formatForErrorMessage());

    return query_node;
}

/// Apply a (sub)query's own query-level `SETTINGS` clause to `updated_context` and append what was
/// applied to `settings_changes` / `default_settings` / `query_parameters`, so that it can be
/// persisted on the query tree node it belongs to (and therefore survive cloning and the conversion
/// back to AST for distributed execution). Applying and recording are kept together here because
/// both a `SELECT`'s clause and a `UNION`'s query-level clause must be handled identically.
void applyQueryLevelSettings(
    ASTSetQuery & set_query,
    ContextMutablePtr & updated_context,
    SettingsChanges & settings_changes,
    std::vector<String> & default_settings,
    NameToNameVector & query_parameters)
{
    /// The parser accepts `SETTINGS name` without a value for any setting - it does not know the
    /// settings schema - so the shorthand has to be rejected here, against the schema, before
    /// `limit` and `offset` are dropped below. For a nested subquery this is the first place the
    /// inner `SETTINGS` clause is seen with the schema at hand, and the two are dropped without
    /// being read, so without this a valueless `SETTINGS limit` would be silently accepted.
    updated_context->getSettingsRef().checkShorthandChanges(set_query.changes);

    /// `limit` / `offset` settings are materialized earlier by wrapping the query - including
    /// subqueries that carry the setting in their own `SETTINGS` clause - as a derived table with an
    /// outer `LIMIT` / `OFFSET` (`applyQueryConstructionSettings` / `wrapNestedConstructionSettings`
    /// in `executeQuery` for directly executed queries; `wrapNestedConstructionSettings` again in
    /// `TableFunctionEval` for a generated query that bypasses `executeQuery`). By the time the query
    /// tree is built the settings are therefore already consumed; drop any that remain so they are not
    /// re-applied to this (sub)query's context - the query tree's `LIMIT` / `OFFSET` come from the SQL
    /// clauses.
    set_query.changes.removeSetting("limit");
    set_query.changes.removeSetting("offset");

    /// A nested `SETTINGS` clause (a subquery, a CTE, a view's inner query) is clamped to the session's
    /// settings constraints, as done for other settings crossing execution contexts
    /// (`getSQLSecurityOverriddenContext`, secondary queries, DDL replay): violating changes are dropped,
    /// out-of-bounds values are clamped, so a view whose inner clause violates the reader's constraints
    /// keeps working. A top-level clause still throws (`applySettingsFromQuery`). An unknown name or an
    /// uncastable value throws as for a top-level clause, on every server that executes the query. The
    /// node records the effective `changes`: the ones that survive, with their clamped values and the
    /// literals as written, so `toAST`, the tree hash, the `Planner` and the shards receiving the
    /// subquery see what was applied. The changes are clamped one at a time, so that each is clamped
    /// against the context as the preceding changes left it - the last-wins reading the clause has
    /// everywhere else.
    ///
    /// `SETTINGS x = DEFAULT` is parsed into `default_settings`, not `changes`. Those settings are reset
    /// in the subquery scope as well, so that the reset actually takes effect for the subquery (e.g. a
    /// table function such as `obfuscate` reading the effective setting). A reset is an assignment of
    /// the declared default, so it can violate the reader's constraints exactly like an explicit
    /// assignment - without clamping, an inner `SETTINGS max_threads = DEFAULT` would escape a
    /// `readonly`, `CONST` or `MIN` / `MAX` constraint that the same clause could not escape by writing
    /// the value out. The top-level path throws (`checkSettingsConstraintsForSettingsReset` in
    /// `InterpreterSetQuery`); here a forbidden reset is dropped, and a reset past a bound becomes an
    /// assignment of the clamped value. The node keeps the resets as written.
    ///
    /// The sequence is shared with `ApplyWithSubqueryVisitor` and the query result cache check
    /// (`Context::applyNestedQuerySettingsClamped`).
    auto effective_changes = updated_context->applyNestedQuerySettingsClamped(set_query.changes, set_query.default_settings);
    settings_changes.insert(settings_changes.end(), effective_changes.begin(), effective_changes.end());
    default_settings.insert(default_settings.end(), set_query.default_settings.begin(), set_query.default_settings.end());

    if (!set_query.query_parameters.empty())
    {
        query_parameters.insert(query_parameters.end(), set_query.query_parameters.begin(), set_query.query_parameters.end());
        /// `ParserSetQuery` appends one entry per occurrence, and `ReplaceQueryParameterVisitor`
        /// substitutes with last-wins semantics (`insert_or_assign` over the same vector). Build the
        /// map with the same last-wins rule - a range-constructed `NameToNameMap` would keep the FIRST
        /// binding, so `SETTINGS param_x = '1', param_x = '2'` would substitute `{x:...}` as `2` in the
        /// AST while `Context::getQueryParameters` still reported `1`.
        NameToNameMap query_parameters_map;
        for (const auto & [parameter_name, parameter_value] : set_query.query_parameters)
            query_parameters_map.insert_or_assign(parameter_name, parameter_value);
        updated_context->addQueryParameters(query_parameters_map);
    }

    /// The clause of a query inside the body of a `SQL SECURITY DEFINER` / `NONE` view must not turn parallel replicas back on.
    if (updated_context->isSQLSecurityOverriddenBody())
        updated_context->restrictParallelReplicasInSQLSecurityOverriddenBody();
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectWithUnionExpression(
    const ASTPtr & select_with_union_query,
    bool is_subquery,
    const CommonTableExpressionData & cte_data,
    const ASTPtr & aliases,
    const ContextPtr & context) const
{
    auto & select_with_union_query_typed = select_with_union_query->as<ASTSelectWithUnionQuery &>();
    auto & select_lists = select_with_union_query_typed.list_of_selects->as<ASTExpressionList &>();

    if (select_lists.children.size() == 1)
        return buildSelectOrUnionExpression(select_lists.children[0], is_subquery, cte_data, aliases, context);

    /// A `SETTINGS` clause of a UNION is query-level: it applies to the whole union, not to a single
    /// arm. The grammar gives it two shapes - `ASTQueryWithOutput::settings_ast` for the top-level
    /// query, and, for every other position (a subquery, a table function argument, a CTE), the last
    /// arm's own `SETTINGS` clause, which is where `ParserSelectQuery` leaves a trailing clause (possibly
    /// nested into `INTERSECT` / `EXCEPT`, see `ASTSelectIntersectExceptQuery::getTrailingSettings`). Both
    /// are applied to the union's context, and that context is the one every arm is built from, so a
    /// nested `(SELECT ... UNION ALL SELECT ... SETTINGS x = ...)` shapes ALL of its arms - the same
    /// contract `InterpreterSetQuery::applySettingsFromQuery` implements for a directly executed
    /// query. They are also persisted on the `UnionNode` so that the union keeps its settings through
    /// cloning and query-tree comparison (see `UnionNode::toASTImpl` for the AST direction).
    auto updated_context = Context::createCopy(context);
    SettingsChanges settings_changes;
    std::vector<String> default_settings;
    NameToNameVector query_parameters;

    auto apply_settings = [&](const ASTPtr & settings_ast)
    {
        if (auto * set_query = settings_ast ? settings_ast->as<ASTSetQuery>() : nullptr)
            applyQueryLevelSettings(*set_query, updated_context, settings_changes, default_settings, query_parameters);
    };

    apply_settings(select_with_union_query_typed.settings_ast);
    apply_settings(ASTSelectIntersectExceptQuery::getTrailingSettings(select_lists.children.back()));

    auto union_node = std::make_shared<UnionNode>(
        std::move(updated_context),
        select_with_union_query_typed.union_mode,
        std::move(settings_changes),
        std::move(default_settings),
        std::move(query_parameters));
    union_node->setIsSubquery(is_subquery);
    union_node->setIsCTE(!cte_data.cte_name.empty());
    union_node->setCTEName(std::string(cte_data.cte_name));
    union_node->setIsMaterialized(cte_data.is_materialized);
    union_node->setOriginalAST(select_with_union_query);

    auto union_context = union_node->getContext();
    size_t select_lists_children_size = select_lists.children.size();

    for (size_t i = 0; i < select_lists_children_size; ++i)
    {
        auto & select_list_node = select_lists.children[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/, aliases, union_context);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectIntersectExceptQuery(
    const ASTPtr & select_intersect_except_query,
    bool is_subquery,
    const CommonTableExpressionData & cte_data,
    const ASTPtr & aliases,
    const ContextPtr & context) const
{
    auto & select_intersect_except_query_typed = select_intersect_except_query->as<ASTSelectIntersectExceptQuery &>();
    auto select_lists = select_intersect_except_query_typed.getListOfSelects();

    if (select_lists.size() == 1)
        return buildSelectExpression(select_lists[0], is_subquery, cte_data, aliases, context);

    SelectUnionMode union_mode = {};
    if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_ALL)
        union_mode = SelectUnionMode::INTERSECT_ALL;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::INTERSECT_DISTINCT)
        union_mode = SelectUnionMode::INTERSECT_DISTINCT;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_ALL)
        union_mode = SelectUnionMode::EXCEPT_ALL;
    else if (select_intersect_except_query_typed.final_operator == ASTSelectIntersectExceptQuery::Operator::EXCEPT_DISTINCT)
        union_mode = SelectUnionMode::EXCEPT_DISTINCT;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION type is not initialized");

    /// Exactly as for a `UNION` (see `buildSelectWithUnionExpression`), a trailing `SETTINGS` clause
    /// of a nested `INTERSECT` / `EXCEPT` is query-level: `ParserSelectQuery` leaves it on the last
    /// operand's `ASTSelectQuery`, and it must shape the whole set operation. Apply it to the
    /// context every operand is built from and persist it on the `UnionNode` so that it survives
    /// cloning and query-tree comparison (the last operand's `QueryNode` keeps the clause for the
    /// AST round-trip, so `UnionNode::toASTImpl` must not write it back).
    auto updated_context = Context::createCopy(context);
    SettingsChanges settings_changes;
    std::vector<String> default_settings;
    NameToNameVector query_parameters;

    if (auto trailing_settings = ASTSelectIntersectExceptQuery::getTrailingSettings(select_lists.back()))
    {
        if (auto * set_query = trailing_settings->as<ASTSetQuery>())
            applyQueryLevelSettings(*set_query, updated_context, settings_changes, default_settings, query_parameters);
    }

    auto union_node = std::make_shared<UnionNode>(
        std::move(updated_context),
        union_mode,
        std::move(settings_changes),
        std::move(default_settings),
        std::move(query_parameters));
    union_node->setIsSubquery(is_subquery);
    union_node->setIsCTE(!cte_data.cte_name.empty());
    union_node->setCTEName(std::string(cte_data.cte_name));
    union_node->setIsMaterialized(cte_data.is_materialized);
    union_node->setOriginalAST(select_intersect_except_query);

    auto union_context = union_node->getContext();
    size_t select_lists_size = select_lists.size();

    for (size_t i = 0; i < select_lists_size; ++i)
    {
        auto & select_list_node = select_lists[i];
        QueryTreeNodePtr query_node = buildSelectOrUnionExpression(select_list_node, false /*is_subquery*/, {} /*cte_name*/, aliases, union_context);
        union_node->getQueries().getNodes().push_back(std::move(query_node));
    }

    return union_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildSelectExpression(
    const ASTPtr & select_query,
    bool is_subquery,
    const CommonTableExpressionData & cte_data,
    const ASTPtr & aliases,
    const ContextPtr & context) const
{
    const auto & select_query_typed = select_query->as<ASTSelectQuery &>();

    auto updated_context = Context::createCopy(context);
    if (select_query_typed.is_view_inner_query)
        updated_context->setIsViewInnerQuery(true);
    auto select_settings = select_query_typed.settings();
    SettingsChanges settings_changes;
    std::vector<String> default_settings;
    NameToNameVector query_parameters;

    if (select_settings)
        applyQueryLevelSettings(
            select_settings->as<ASTSetQuery &>(), updated_context, settings_changes, default_settings, query_parameters);

    const auto enable_order_by_all = updated_context->getSettingsRef()[Setting::enable_order_by_all];
    const auto allow_experimental_shuffle_query = updated_context->getSettingsRef()[Setting::allow_experimental_shuffle_query];

    auto current_query_tree = std::make_shared<QueryNode>(
        std::move(updated_context), std::move(settings_changes), std::move(default_settings), std::move(query_parameters));

    current_query_tree->setIsSubquery(is_subquery);
    current_query_tree->setIsCTE(!cte_data.cte_name.empty());
    current_query_tree->setCTEName(std::string(cte_data.cte_name));
    current_query_tree->setIsMaterialized(cte_data.is_materialized);
    current_query_tree->setIsRecursiveWith(select_query_typed.recursive_with);
    current_query_tree->setIsDistinct(select_query_typed.distinct);
    current_query_tree->setIsLimitWithTies(select_query_typed.limit_with_ties);
    current_query_tree->setIsGroupByWithTotals(select_query_typed.group_by_with_totals);
    current_query_tree->setIsGroupByWithCube(select_query_typed.group_by_with_cube);
    current_query_tree->setIsGroupByWithRollup(select_query_typed.group_by_with_rollup);
    current_query_tree->setIsGroupByWithGroupingSets(select_query_typed.group_by_with_grouping_sets);
    current_query_tree->setIsGroupByAll(select_query_typed.group_by_all);
    current_query_tree->setIsLimitByAll(select_query_typed.limit_by_all);

    if (select_query_typed.shuffle && !allow_experimental_shuffle_query)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for `SHUFFLE` is disabled (turn on setting `allow_experimental_shuffle_query`)");

    current_query_tree->setIsShuffle(select_query_typed.shuffle);
    current_query_tree->setIsLimitAfterAll(select_query_typed.limit_after_all);
    /// order_by_all flag in AST is set w/o consideration of `enable_order_by_all` setting
    /// since SETTINGS section has not been parsed yet, - so, check the setting here
    bool order_by_all_enabled = select_query_typed.order_by_all && enable_order_by_all;
    if (order_by_all_enabled)
        current_query_tree->setIsOrderByAll(true);
    current_query_tree->setOriginalAST(select_query);

    auto current_context = current_query_tree->getContext();

    current_query_tree->getJoinTreeNode() = buildJoinTree(is_subquery, select_query_typed, current_context);

    auto select_with_list = select_query_typed.with();
    if (select_with_list)
    {
        current_query_tree->getWithNode() = buildExpressionList(select_with_list, current_context);

        if (select_query_typed.recursive_with)
        {
            for (auto & with_node : current_query_tree->getWith().getNodes())
            {
                auto * with_union_node = with_node->as<UnionNode>();
                if (!with_union_node)
                    continue;

                with_union_node->setIsRecursiveCTE(true);
            }
        }
    }

    auto select_expression_list = select_query_typed.select();
    if (select_expression_list)
        current_query_tree->getProjectionNode() = buildExpressionList(select_expression_list, current_context);

    // Apply the override aliases to the projection nodes
    if (aliases)
        current_query_tree->setProjectionAliasesToOverride(getColumnAliasNames(aliases));

    auto prewhere_expression = select_query_typed.prewhere();
    if (prewhere_expression)
        current_query_tree->getPrewhere() = buildExpression(prewhere_expression, current_context);

    auto where_expression = select_query_typed.where();
    if (where_expression)
        current_query_tree->getWhere() = buildExpression(where_expression, current_context);

    auto group_by_list = select_query_typed.groupBy();
    if (group_by_list)
    {
        auto & group_by_children = group_by_list->children;

        if (current_query_tree->isGroupByWithGroupingSets())
        {
            auto grouping_sets_list_node = std::make_shared<ListNode>();

            for (auto & grouping_sets_keys : group_by_children)
            {
                auto grouping_sets_keys_list_node = buildExpressionList(grouping_sets_keys, current_context);
                current_query_tree->getGroupBy().getNodes().emplace_back(std::move(grouping_sets_keys_list_node));
            }
        }
        else
        {
            current_query_tree->getGroupByNode() = buildExpressionList(group_by_list, current_context);
        }
    }

    auto having_expression = select_query_typed.having();
    if (having_expression)
        current_query_tree->getHaving() = buildExpression(having_expression, current_context);

    auto window_list = select_query_typed.window();
    if (window_list)
        current_query_tree->getWindowNode() = buildWindowList(window_list, current_context);

    auto qualify_expression = select_query_typed.qualify();
    if (qualify_expression)
        current_query_tree->getQualify() = buildExpression(qualify_expression, current_context);

    auto select_order_by_list = select_query_typed.orderBy();
    if (select_order_by_list)
    {
        if (order_by_all_enabled)
        {
            /// ORDER BY ALL is enabled. Create a placeholder SortNode with the correct direction.
            /// The actual expansion to all SELECT columns will happen in QueryAnalyzer::expandOrderByAll.
            auto * all_elem = select_order_by_list->children[0]->as<ASTOrderByElement>();

            auto sort_direction = all_elem->direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;
            std::optional<SortDirection> nulls_sort_direction;
            if (all_elem->nulls_direction_was_explicitly_specified)
                nulls_sort_direction = all_elem->nulls_direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;

            auto placeholder_expr = std::make_shared<ConstantNode>(Field{"__order_by_all_placeholder__"});
            auto sort_node = std::make_shared<SortNode>(std::move(placeholder_expr), sort_direction, nulls_sort_direction);

            auto list_node = std::make_shared<ListNode>();
            list_node->getNodes().push_back(std::move(sort_node));
            current_query_tree->getOrderByNode() = std::move(list_node);
        }
        else if (select_query_typed.order_by_all)
        {
            /// ORDER BY ALL was parsed but `enable_order_by_all` is disabled.
            /// Build an ORDER BY list with `all` as a column reference.
            auto * all_elem = select_order_by_list->children[0]->as<ASTOrderByElement>();

            auto order_by_list = make_intrusive<ASTExpressionList>();
            auto elem = make_intrusive<ASTOrderByElement>();
            elem->direction = all_elem->direction;
            elem->nulls_direction = all_elem->nulls_direction;
            elem->nulls_direction_was_explicitly_specified = all_elem->nulls_direction_was_explicitly_specified;
            elem->children.push_back(make_intrusive<ASTIdentifier>("all"));
            order_by_list->children.push_back(std::move(elem));

            current_query_tree->getOrderByNode() = buildSortList(order_by_list, current_context);
        }
        else
        {
            current_query_tree->getOrderByNode() = buildSortList(select_order_by_list, current_context);
        }
    }

    auto interpolate_list = select_query_typed.interpolate();
    if (interpolate_list)
        current_query_tree->getInterpolate() = buildInterpolateList(interpolate_list, current_context);

    auto select_limit_by_limit = select_query_typed.limitByLength();
    if (select_limit_by_limit)
        current_query_tree->getLimitByLimit() = buildExpression(select_limit_by_limit, current_context);

    auto select_limit_by_offset = select_query_typed.limitByOffset();
    if (select_limit_by_offset)
        current_query_tree->getLimitByOffset() = buildExpression(select_limit_by_offset, current_context);

    auto select_limit_by = select_query_typed.limitBy();
    if (select_limit_by)
        current_query_tree->getLimitByNode() = buildExpressionList(select_limit_by, current_context);

    auto select_limit_after = select_query_typed.limitAfter();
    if (select_limit_after)
        current_query_tree->getLimitAfter() = buildExpression(select_limit_after, current_context);

    auto select_limit_until = select_query_typed.limitUntil();
    if (select_limit_until)
        current_query_tree->getLimitUntil() = buildExpression(select_limit_until, current_context);

    /// Combine limit expression with limit and offset settings into final limit expression
    /// `LIMIT` / `OFFSET` come straight from the SQL clauses. The `limit` / `offset` settings are no
    /// longer folded in here — they are materialized as an outer query's `LIMIT` / `OFFSET` by the
    /// subquery-wrapping in `executeQuery` (`applyQueryConstructionSettings` for the top-level query,
    /// `wrapNestedConstructionSettings` for subqueries that carry the setting in their `SETTINGS`
    /// clause), so combining with the setting is left to the optimizer's limit push-down.
    if (auto select_limit = select_query_typed.limitLength())
        current_query_tree->getLimit() = buildExpression(select_limit, current_context);

    if (auto select_offset = select_query_typed.limitOffset())
        current_query_tree->getOffset() = buildExpression(select_offset, current_context);

    return current_query_tree;
}

QueryTreeNodePtr QueryTreeBuilder::buildSortList(const ASTPtr & order_by_expression_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = order_by_expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        const auto & order_by_element = expression->as<const ASTOrderByElement &>();

        auto sort_direction = order_by_element.direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;
        std::optional<SortDirection> nulls_sort_direction;
        if (order_by_element.nulls_direction_was_explicitly_specified)
            nulls_sort_direction = order_by_element.nulls_direction == 1 ? SortDirection::ASCENDING : SortDirection::DESCENDING;

        std::shared_ptr<Collator> collator;
        if (order_by_element.getCollation())
            collator = std::make_shared<Collator>(order_by_element.getCollation()->as<ASTLiteral &>().value.safeGet<String>());

        const auto & sort_expression_ast = order_by_element.children.at(0);
        auto sort_expression = buildExpression(sort_expression_ast, context);
        auto sort_node = std::make_shared<SortNode>(std::move(sort_expression),
            sort_direction,
            nulls_sort_direction,
            std::move(collator),
            order_by_element.with_fill);

        if (order_by_element.getFillFrom())
            sort_node->getFillFrom() = buildExpression(order_by_element.getFillFrom(), context);
        if (order_by_element.getFillTo())
            sort_node->getFillTo() = buildExpression(order_by_element.getFillTo(), context);
        if (order_by_element.getFillStep())
            sort_node->getFillStep() = buildExpression(order_by_element.getFillStep(), context);
        if (order_by_element.getFillStaleness())
            sort_node->getFillStaleness() = buildExpression(order_by_element.getFillStaleness(), context);

        list_node->getNodes().push_back(std::move(sort_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildInterpolateList(const ASTPtr & interpolate_expression_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = interpolate_expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        const auto & interpolate_element = expression->as<const ASTInterpolateElement &>();
        auto expression_to_interpolate = std::make_shared<IdentifierNode>(Identifier(interpolate_element.column));
        auto interpolate_expression = buildExpression(interpolate_element.expr, context);
        auto interpolate_node = std::make_shared<InterpolateNode>(std::move(expression_to_interpolate), std::move(interpolate_expression));

        list_node->getNodes().push_back(std::move(interpolate_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildWindowList(const ASTPtr & window_definition_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = window_definition_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & window_list_element : expression_list_typed.children)
    {
        const auto & window_list_element_typed = window_list_element->as<const ASTWindowListElement &>();

        auto window_node = buildWindow(window_list_element_typed.definition, context);
        window_node->setAlias(window_list_element_typed.name);

        list_node->getNodes().push_back(std::move(window_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpressionList(const ASTPtr & expression_list, const ContextPtr & context) const
{
    auto list_node = std::make_shared<ListNode>();

    auto & expression_list_typed = expression_list->as<ASTExpressionList &>();
    list_node->getNodes().reserve(expression_list_typed.children.size());

    for (auto & expression : expression_list_typed.children)
    {
        auto expression_node = buildExpression(expression, context);
        list_node->getNodes().push_back(std::move(expression_node));
    }

    return list_node;
}

QueryTreeNodePtr QueryTreeBuilder::buildExpression(const ASTPtr & expression, const ContextPtr & context) const
{
    QueryTreeNodePtr result;

    if (const auto * ast_identifier = expression->as<ASTIdentifier>())
    {
        throwIfQueryParameter(*ast_identifier);
        auto identifier = Identifier(ast_identifier->name_parts);
        result = std::make_shared<IdentifierNode>(std::move(identifier));
    }
    else if (const auto * table_identifier = expression->as<ASTTableIdentifier>())
    {
        throwIfQueryParameter(*table_identifier);
        auto identifier = Identifier(table_identifier->name_parts);
        result = std::make_shared<IdentifierNode>(std::move(identifier));
    }
    else if (const auto * asterisk = expression->as<ASTAsterisk>())
    {
        auto column_transformers = buildColumnTransformers(asterisk->transformers, context);
        result = std::make_shared<MatcherNode>(std::move(column_transformers));
    }
    else if (const auto * qualified_asterisk = expression->as<ASTQualifiedAsterisk>())
    {
        auto & qualified_identifier = qualified_asterisk->qualifier->as<ASTIdentifier &>();
        throwIfQueryParameter(qualified_identifier);
        auto column_transformers = buildColumnTransformers(qualified_asterisk->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), std::move(column_transformers));
    }
    else if (const auto * ast_literal = expression->as<ASTLiteral>())
    {
        if (context->getSettingsRef()[Setting::use_variant_as_common_type])
            result = std::make_shared<ConstantNode>(ast_literal->value, applyVisitor(FieldToDataType<LeastSupertypeOnError::Variant>(), ast_literal->value));
        else
            result = std::make_shared<ConstantNode>(ast_literal->value);
    }
    else if (const auto * function = expression->as<ASTFunction>())
    {
        if (function->isLambdaFunction() || isASTLambdaFunction(*function))
        {
            const auto & lambda_arguments_and_expression = function->arguments->as<ASTExpressionList &>().children;
            auto & lambda_arguments_tuple = lambda_arguments_and_expression.at(0)->as<ASTFunction &>();

            Names lambda_arguments;
            NameSet lambda_arguments_set;

            if (lambda_arguments_tuple.arguments)
            {
                const auto & lambda_arguments_list = lambda_arguments_tuple.arguments->as<ASTExpressionList &>().children;
                for (const auto & lambda_argument : lambda_arguments_list)
                {
                    const auto * lambda_argument_identifier = lambda_argument->as<ASTIdentifier>();

                    if (!lambda_argument_identifier)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} argument is not identifier",
                            function->formatForErrorMessage());

                    throwIfQueryParameter(*lambda_argument_identifier);

                    if (lambda_argument_identifier->name_parts.size() > 1)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} argument identifier must contain single part. Actual {}",
                            function->formatForErrorMessage(),
                            lambda_argument_identifier->full_name);

                    const auto & argument_name = lambda_argument_identifier->name_parts[0];
                    auto [_, inserted] = lambda_arguments_set.insert(argument_name);
                    if (!inserted)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Lambda {} multiple arguments with same name {}",
                            function->formatForErrorMessage(),
                            argument_name);

                    lambda_arguments.push_back(argument_name);
                }
            }

            auto lambda_arguments_node = std::make_shared<LambdaArgumentsNode>(std::move(lambda_arguments));

            const auto & lambda_expression = lambda_arguments_and_expression.at(1);
            auto lambda_expression_node = buildExpression(lambda_expression, context);

            result = std::make_shared<LambdaNode>(std::move(lambda_arguments_node), std::move(lambda_expression_node), function->isOperator());
        }
        else
        {
            const auto & group_concat_aliases = GroupConcatImpl<false>::getNameAndAliases();
            if (!function->name.empty() && std::any_of(
                    group_concat_aliases.begin(), group_concat_aliases.end(),
                    [&](const std::string &s) { return Poco::toLower(s) == Poco::toLower(function->name); })
                && function->arguments && function->arguments->children.size() == 2)
            {
                result = setSecondArgumentAsParameter(function, context);
            }
            else
            {
                auto function_node = std::make_shared<FunctionNode>(function->name);
                function_node->setNullsAction(function->getNullsAction());
                function_node->markAsOperator(function->isOperator());

                if (function->parameters)
                {
                    const auto & function_parameters_list = function->parameters->as<ASTExpressionList>()->children;
                    for (const auto & argument : function_parameters_list)
                        function_node->getParameters().getNodes().push_back(buildExpression(argument, context));
                }

                if (function->arguments)
                {
                    const auto & function_arguments_list = function->arguments->as<ASTExpressionList>()->children;
                    for (const auto & argument : function_arguments_list)
                        function_node->getArguments().getNodes().push_back(buildExpression(argument, context));
                }

                if (function->isWindowFunction())
                {
                    if (function->window_definition)
                        function_node->getWindowNode() = buildWindow(function->window_definition, context);
                    else
                        function_node->getWindowNode() = std::make_shared<IdentifierNode>(Identifier(function->window_name));
                }

                result = std::move(function_node);
            }
        }
    }
    else if (const auto * subquery = expression->as<ASTSubquery>())
    {
        auto subquery_query = subquery->children[0];
        auto query_node = buildSelectWithUnionExpression(subquery_query, true /*is_subquery*/, {} /*cte_name*/, nullptr /*aliases*/, context);

        result = std::move(query_node);
    }
    else if (const auto * /*select_with_union_query*/ _ = expression->as<ASTSelectWithUnionQuery>())
    {
        auto query_node = buildSelectWithUnionExpression(expression, false /*is_subquery*/, {} /*cte_name*/, nullptr /*aliases*/, context);
        result = std::move(query_node);
    }
    else if (const auto * with_element = expression->as<ASTWithElement>())
    {
        auto with_element_subquery = with_element->subquery->as<ASTSubquery &>().children.at(0);
        CommonTableExpressionData cte_data = {
            .cte_name = with_element->name,
            .is_materialized = with_element->is_materialized,
        };
        auto query_node = buildSelectWithUnionExpression(with_element_subquery, true /*is_subquery*/, cte_data /*cte_data*/, with_element->aliases /*aliases*/, context);

        result = std::move(query_node);
    }
    else if (const auto * columns_regexp_matcher = expression->as<ASTColumnsRegexpMatcher>())
    {
        auto column_transformers = buildColumnTransformers(columns_regexp_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(columns_regexp_matcher->getPattern(), std::move(column_transformers));
    }
    else if (const auto * columns_list_matcher = expression->as<ASTColumnsListMatcher>())
    {
        Identifiers column_list_identifiers;
        column_list_identifiers.reserve(columns_list_matcher->column_list->children.size());

        for (auto & column_list_child : columns_list_matcher->column_list->children)
        {
            auto & column_list_identifier = column_list_child->as<ASTIdentifier &>();
            throwIfQueryParameter(column_list_identifier);
            column_list_identifiers.emplace_back(Identifier{column_list_identifier.name_parts});
        }

        auto column_transformers = buildColumnTransformers(columns_list_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_regexp_matcher = expression->as<ASTQualifiedColumnsRegexpMatcher>())
    {
        auto & qualified_identifier = qualified_columns_regexp_matcher->qualifier->as<ASTIdentifier &>();
        throwIfQueryParameter(qualified_identifier);
        auto column_transformers = buildColumnTransformers(qualified_columns_regexp_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), qualified_columns_regexp_matcher->getPattern(), std::move(column_transformers));
    }
    else if (const auto * qualified_columns_list_matcher = expression->as<ASTQualifiedColumnsListMatcher>())
    {
        auto & qualified_identifier = qualified_columns_list_matcher->qualifier->as<ASTIdentifier &>();
        throwIfQueryParameter(qualified_identifier);

        Identifiers column_list_identifiers;
        column_list_identifiers.reserve(qualified_columns_list_matcher->column_list->children.size());

        for (auto & column_list_child : qualified_columns_list_matcher->column_list->children)
        {
            auto & column_list_identifier = column_list_child->as<ASTIdentifier &>();
            throwIfQueryParameter(column_list_identifier);
            column_list_identifiers.emplace_back(Identifier{column_list_identifier.name_parts});
        }

        auto column_transformers = buildColumnTransformers(qualified_columns_list_matcher->transformers, context);
        result = std::make_shared<MatcherNode>(Identifier(qualified_identifier.name_parts), std::move(column_list_identifiers), std::move(column_transformers));
    }
    else if (const auto * query_parameter = expression->as<ASTQueryParameter>())
    {
        throw Exception(ErrorCodes::UNKNOWN_QUERY_PARAMETER,
            "Query parameter {} was not set",
            backQuote(query_parameter->name));
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Invalid expression. Expected identifier, literal, matcher, function, subquery. Actual {}",
            expression->formatForErrorMessage());
    }

    result->setAlias(expression->tryGetAlias());
    result->setOriginalAST(expression);
    result->setParenthesized(expression->isParenthesized());

    return result;
}

QueryTreeNodePtr QueryTreeBuilder::buildWindow(const ASTPtr & window_definition, const ContextPtr & context) const
{
    const auto & window_definition_typed = window_definition->as<const ASTWindowDefinition &>();
    WindowFrame window_frame;

    if (!window_definition_typed.frame_is_default)
    {
        window_frame.is_default = false;
        window_frame.type = window_definition_typed.frame_type;
        window_frame.begin_type = window_definition_typed.frame_begin_type;
        window_frame.begin_preceding = window_definition_typed.frame_begin_preceding;
        window_frame.end_type = window_definition_typed.frame_end_type;
        window_frame.end_preceding = window_definition_typed.frame_end_preceding;
    }

    auto window_node = std::make_shared<WindowNode>(window_frame);
    window_node->setParentWindowName(window_definition_typed.parent_window_name);

    if (window_definition_typed.partition_by)
        window_node->getPartitionByNode() = buildExpressionList(window_definition_typed.partition_by, context);

    if (window_definition_typed.order_by)
        window_node->getOrderByNode() = buildSortList(window_definition_typed.order_by, context);

    if (window_definition_typed.frame_begin_offset)
        window_node->getFrameBeginOffsetNode() = buildExpression(window_definition_typed.frame_begin_offset, context);

    if (window_definition_typed.frame_end_offset)
        window_node->getFrameEndOffsetNode() = buildExpression(window_definition_typed.frame_end_offset, context);

    window_node->setOriginalAST(window_definition);

    return window_node;
}

std::shared_ptr<TableFunctionNode> QueryTreeBuilder::buildTableFunction(const ASTPtr & table_function, const ContextPtr & context) const
{
    auto & table_function_expression = table_function->as<ASTFunction &>();

    auto node = std::make_shared<TableFunctionNode>(table_function_expression.name);

    if (table_function_expression.arguments)
    {
        const auto & function_arguments_list = table_function_expression.arguments->as<ASTExpressionList &>().children;
        for (const auto & argument : function_arguments_list)
        {
            if (!node->getSettingsChanges().empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' has arguments after SETTINGS",
                    table_function_expression.formatForErrorMessage());

            if (argument->as<ASTSelectQuery>() || argument->as<ASTSelectWithUnionQuery>() || argument->as<ASTSelectIntersectExceptQuery>())
                node->getArguments().getNodes().push_back(buildSelectOrUnionExpression(argument, false /*is_subquery*/, {} /*cte_name*/, nullptr /*aliases*/, context));
            else if (const auto * ast_set = argument->as<ASTSetQuery>())
                node->setSettingsChanges(ast_set->changes);
            else
                node->getArguments().getNodes().push_back(buildExpression(argument, context));
        }
    }

    return node;
}

QueryTreeNodePtr QueryTreeBuilder::buildJoinTree(bool is_subquery, const ASTSelectQuery & select_query, const ContextPtr & context) const
{
    const auto & tables_in_select_query = select_query.tables();
    if (!tables_in_select_query)
    {
        /** If no table is specified in SELECT query,
          * if 'implicit_table_at_top_level' is set, we substitute it as a table,
          * otherwise, we substitute the system.one table: SELECT * FROM system.one;
          */
        if (!is_subquery)
        {
            const String & implicit_table = context->getSettingsRef()[Setting::implicit_table_at_top_level];
            if (!implicit_table.empty())
            {
                /// Parse the value as a (possibly back-quoted) compound identifier rather than splitting
                /// the raw string on every `.`. This lets a single name part contain a literal dot when
                /// it is back-quoted — e.g. `db`.`my.table` resolves to database `db`, table `my.table`,
                /// not the three parts `db`, `my`, `table`. A plain unquoted `db.table` parses to the
                /// same parts as before, so existing callers are unaffected.
                const auto & settings = context->getSettingsRef();
                ParserCompoundIdentifier parser;
                ASTPtr identifier_ast = parseQuery(
                    parser, implicit_table.data(), implicit_table.data() + implicit_table.size(),
                    "implicit_table_at_top_level setting",
                    settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
                return std::make_shared<IdentifierNode>(Identifier(identifier_ast->as<ASTIdentifier &>().name_parts));
            }
        }

        return std::make_shared<IdentifierNode>(Identifier("system.one"));
    }

    auto & tables = tables_in_select_query->as<ASTTablesInSelectQuery &>();

    QueryTreeNodes table_expressions;
    bool pad_unaligned = context->getSettingsRef()[Setting::enable_unaligned_array_join];

    for (const auto & table_element_untyped : tables.children)
    {
        const auto & table_element = table_element_untyped->as<ASTTablesInSelectQueryElement &>();

        std::optional<UnnestTableExpression> unnest;
        if (table_element.table_expression)
            unnest = tryGetUnnestTableExpression(table_element.table_expression->as<ASTTableExpression &>());
        bool is_lateral_unnest = unnest && table_element.table_join && isLateralUnnestJoin(table_element.table_join->as<ASTTableJoin &>());

        if (table_element.table_expression)
        {
            auto & table_expression = table_element.table_expression->as<ASTTableExpression &>();
            std::optional<TableExpressionModifiers> table_expression_modifiers;

            if (table_expression.final || table_expression.sample_size || table_expression.stream_settings)
            {
                bool has_final = table_expression.final;
                std::optional<TableExpressionModifiers::Rational> sample_size_ratio;
                std::optional<TableExpressionModifiers::Rational> sample_offset_ratio;
                std::optional<StreamSettings> stream_settings;

                if (table_expression.sample_size)
                {
                    auto & ast_sample_size_ratio = table_expression.sample_size->as<ASTSampleRatio &>();
                    sample_size_ratio = ast_sample_size_ratio.ratio;

                    if (table_expression.sample_offset)
                    {
                        auto & ast_sample_offset_ratio = table_expression.sample_offset->as<ASTSampleRatio &>();
                        sample_offset_ratio = ast_sample_offset_ratio.ratio;
                    }
                }

                if (table_expression.stream_settings)
                {
                    const auto & ast_stream_settings = table_expression.stream_settings->as<ASTStreamSettings &>();
                    stream_settings = StreamSettings{};
                    stream_settings->subscribe_for_updates = ast_stream_settings.subscribe_for_updates;
                    stream_settings->unordered = ast_stream_settings.unordered;
                    stream_settings->cursor = ast_stream_settings.cursor;
                    stream_settings->watermark = ast_stream_settings.watermark;
                }

                table_expression_modifiers = TableExpressionModifiers(has_final, sample_size_ratio, sample_offset_ratio, std::move(stream_settings));
            }

            if (unnest)
            {
                if (table_expression_modifiers)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Table expression modifiers {} are not supported for {}",
                        table_expression_modifiers->formatForErrorMessage(),
                        table_expression.table_function->formatForErrorMessage());

                /// A lateral `unnest` becomes an ARRAY JOIN of the preceding tables, built with the join below.
                if (!is_lateral_unnest)
                {
                    auto subquery = buildUnnestSubquery(*unnest, pad_unaligned);
                    auto node = buildSelectWithUnionExpression(subquery, true /*is_subquery*/, {} /*cte_data*/, select_query.aliases(), context);
                    node->setAlias(unnest->table_alias);
                    node->setOriginalAST(subquery);

                    table_expressions.push_back(std::move(node));
                }
            }
            else if (table_expression.database_and_table_name)
            {
                auto & table_identifier_typed = table_expression.database_and_table_name->as<ASTTableIdentifier &>();
                throwIfQueryParameter(table_identifier_typed);
                auto storage_identifier = Identifier(table_identifier_typed.name_parts);
                QueryTreeNodePtr table_identifier_node;

                if (table_expression_modifiers)
                    table_identifier_node = std::make_shared<IdentifierNode>(storage_identifier, *table_expression_modifiers);
                else
                    table_identifier_node = std::make_shared<IdentifierNode>(storage_identifier);

                table_identifier_node->setAlias(table_identifier_typed.tryGetAlias());
                table_identifier_node->setOriginalAST(table_element.table_expression);

                table_expressions.push_back(std::move(table_identifier_node));
            }
            else if (table_expression.subquery)
            {
                auto & subquery_expression = table_expression.subquery->as<ASTSubquery &>();
                const auto & select_with_union_query = subquery_expression.children[0];

                /// Views store CTE references in FROM as subqueries with cte_name set (ApplyWithSubqueryVisitor).
                /// Propagate it so qualified identifiers like `cte_name.column` still bind, as they do when
                /// a CTE reference is resolved from the WITH section directly.
                auto node = buildSelectWithUnionExpression(
                    select_with_union_query,
                    true /*is_subquery*/,
                    CommonTableExpressionData{.cte_name = subquery_expression.cte_name},
                    select_query.aliases(),
                    context);
                node->setAlias(subquery_expression.tryGetAlias());
                node->setOriginalAST(select_with_union_query);

                /// Apply column aliases from AS alias(col1, col2, ...) syntax
                if (table_expression.column_aliases)
                {
                    Names column_alias_names = getColumnAliasNames(table_expression.column_aliases);

                    if (auto * query_node = node->as<QueryNode>())
                    {
                        query_node->setProjectionAliasesToOverride(std::move(column_alias_names));
                    }
                    else if (auto * union_node = node->as<UnionNode>())
                    {
                        /// for UNIONs, apply aliases to the first query in the union, projection column names come from the first query (see UnionNode::computeProjectionColumns)
                        /// we find the first QueryNode in case of nested UNIONs
                        const auto & queries = union_node->getQueries().getNodes();
                        QueryTreeNodePtr current = queries.empty() ? nullptr : queries[0];
                        while (current)
                        {
                            if (auto * inner_query = current->as<QueryNode>())
                            {
                                inner_query->setProjectionAliasesToOverride(std::move(column_alias_names));
                                break;
                            }
                            else if (auto * inner_union = current->as<UnionNode>())
                            {
                                const auto & inner_queries = inner_union->getQueries().getNodes();
                                current = inner_queries.empty() ? nullptr : inner_queries[0];
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                }

                if (table_expression_modifiers)
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Table expression modifiers {} are not supported for subquery {}",
                        table_expression_modifiers->formatForErrorMessage(),
                        node->formatASTForErrorMessage());
                }

                table_expressions.push_back(std::move(node));
            }
            else if (table_expression.table_function)
            {
                if (table_expression.with_ordinality)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH ORDINALITY is supported only for unnest, not for {}",
                        table_expression.table_function->formatForErrorMessage());

                auto node = buildTableFunction(table_expression.table_function, context);

                if (table_expression_modifiers)
                    node->setTableExpressionModifiers(*table_expression_modifiers);
                node->setAlias(table_expression.table_function->tryGetAlias());
                node->setOriginalAST(table_expression.table_function);

                table_expressions.push_back(std::move(node));
            }
            else
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported table expression node {}",
                                table_element.table_expression->formatForErrorMessage());
            }
        }

        if (is_lateral_unnest)
        {
            const auto & table_join = table_element.table_join->as<ASTTableJoin &>();

            auto left_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            auto array_join_expressions = buildUnnestArrayJoinExpressions(*unnest, table_join.on_expression, pad_unaligned);
            auto array_join_node = std::make_shared<ArrayJoinNode>(
                std::move(left_table_expression), buildExpressionList(array_join_expressions, context), table_join.kind == JoinKind::Left);
            array_join_node->setUnnestAlias(unnest->table_alias);

            table_expressions.push_back(std::move(array_join_node));
            continue;
        }

        if (table_element.table_join)
        {
            const auto & table_join = table_element.table_join->as<ASTTableJoin &>();

            auto right_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            auto left_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            QueryTreeNodePtr join_expression;

            if (table_join.using_expression_list)
                join_expression = buildExpressionList(table_join.using_expression_list, context);
            else if (table_join.on_expression)
                join_expression = buildExpression(table_join.on_expression, context);

            const auto & settings = context->getSettingsRef();
            auto join_default_strictness = settings[Setting::join_default_strictness];
            auto any_join_distinct_right_table_keys = settings[Setting::any_join_distinct_right_table_keys];

            JoinStrictness result_join_strictness = table_join.strictness;
            JoinKind result_join_kind = table_join.kind;

            /// `LATERAL JOIN` supports only `ALL` semantics, so an unspecified strictness must not
            /// depend on `join_default_strictness` or `any_join_distinct_right_table_keys`.
            if (table_join.lateral && result_join_strictness == JoinStrictness::Unspecified
                && result_join_kind != JoinKind::Cross && result_join_kind != JoinKind::Comma)
                result_join_strictness = JoinStrictness::All;

            if (result_join_strictness == JoinStrictness::Unspecified && (result_join_kind != JoinKind::Cross && result_join_kind != JoinKind::Comma))
            {
                if (join_default_strictness == JoinStrictness::Any)
                    result_join_strictness = JoinStrictness::Any;
                else if (join_default_strictness == JoinStrictness::All)
                    result_join_strictness = JoinStrictness::All;
                else
                    throw Exception(ErrorCodes::EXPECTED_ALL_OR_ANY,
                        "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty");
            }

            if (any_join_distinct_right_table_keys)
            {
                if (result_join_strictness == JoinStrictness::Any && result_join_kind == JoinKind::Inner)
                {
                    result_join_strictness = JoinStrictness::Semi;
                    result_join_kind = JoinKind::Left;
                }

                if (result_join_strictness == JoinStrictness::Any)
                    result_join_strictness = JoinStrictness::RightAny;
            }
            else if (result_join_strictness == JoinStrictness::Any && result_join_kind == JoinKind::Full)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ANY FULL JOINs are not implemented");
            }

            QueryTreeNodePtr join_node;
            if (result_join_kind == JoinKind::Cross || result_join_kind == JoinKind::Comma)
            {
                CrossJoinNode * cross_join = nullptr;
                if (auto * left_cross_join = left_table_expression->as<CrossJoinNode>())
                    cross_join = left_cross_join;
                else
                {
                    auto new_cross_join = std::make_shared<CrossJoinNode>(std::move(left_table_expression));
                    cross_join = new_cross_join.get();
                    left_table_expression = std::move(new_cross_join);
                }

                CrossJoinNode::JoinType join_type{result_join_kind == JoinKind::Comma, table_join.locality};

                if (auto * right_cross_join = right_table_expression->as<CrossJoinNode>())
                {
                    const auto & expr = right_cross_join->getTableExpressions();
                    cross_join->appendTable(expr.front(), join_type);

                    const auto & join_types = right_cross_join->getJoinTypes();
                    for (size_t i = 0; i < join_types.size(); ++i)
                        cross_join->appendTable(expr[i + 1], join_types[i]);
                }
                else
                    cross_join->appendTable(right_table_expression, join_type);

                join_node = std::move(left_table_expression);
            }
            else
            {
                join_node = std::make_shared<JoinNode>(std::move(left_table_expression),
                    std::move(right_table_expression),
                    std::move(join_expression),
                    table_join.locality,
                    result_join_strictness,
                    result_join_kind,
                    table_join.using_expression_list != nullptr);
                join_node->as<JoinNode &>().setNatural(table_join.is_natural);
                join_node->as<JoinNode &>().setLateral(table_join.lateral);
            }

            join_node->setOriginalAST(table_element.table_join);

            /** Original AST is not set because it will contain only join part and does
              * not include left table expression.
              */
            table_expressions.emplace_back(std::move(join_node));
        }

        if (table_element.array_join)
        {
            auto & array_join_expression = table_element.array_join->as<ASTArrayJoin &>();
            bool is_left_array_join = array_join_expression.kind == ASTArrayJoin::Kind::Left;

            auto last_table_expression = std::move(table_expressions.back());
            table_expressions.pop_back();

            auto array_join_expressions_list = buildExpressionList(array_join_expression.expression_list, context);
            auto array_join_node = std::make_shared<ArrayJoinNode>(std::move(last_table_expression), std::move(array_join_expressions_list), is_left_array_join);

            /** Original AST is not set because it will contain only array join part and does
              * not include left table expression.
              */
            table_expressions.push_back(std::move(array_join_node));
        }
    }

    if (table_expressions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query FROM section cannot be empty");

    if (table_expressions.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query FROM section cannot have more than 1 root table expression");

    return table_expressions.back();
}


ColumnTransformersNodes QueryTreeBuilder::buildColumnTransformers(const ASTPtr & matcher_expression, const ContextPtr & context) const
{
    ColumnTransformersNodes column_transformers;

    if (!matcher_expression)
        return column_transformers;

    for (const auto & child : matcher_expression->children)
    {
        if (auto * apply_transformer = child->as<ASTColumnsApplyTransformer>())
        {
            if (apply_transformer->lambda)
            {
                auto lambda_query_tree_node = buildExpression(apply_transformer->lambda, context);
                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(lambda_query_tree_node), apply_transformer->column_name_prefix));
            }
            else
            {
                auto function_node = std::make_shared<FunctionNode>(apply_transformer->func_name);
                if (apply_transformer->parameters)
                    function_node->getParametersNode() = buildExpressionList(apply_transformer->parameters, context);

                column_transformers.emplace_back(std::make_shared<ApplyColumnTransformerNode>(std::move(function_node), apply_transformer->column_name_prefix));
            }
        }
        else if (auto * except_transformer = child->as<ASTColumnsExceptTransformer>())
        {
            auto matcher = getColumnsExceptMatcher(*except_transformer);
            if (matcher)
            {
                column_transformers.emplace_back(std::make_shared<ExceptColumnTransformerNode>(std::move(matcher)));
            }
            else
            {
                Names except_column_names;
                except_column_names.reserve(except_transformer->children.size());

                for (auto & except_transformer_child : except_transformer->children)
                {
                    const auto & except_identifier = except_transformer_child->as<ASTIdentifier &>();
                    throwIfQueryParameter(except_identifier);
                    except_column_names.push_back(except_identifier.full_name);
                }

                column_transformers.emplace_back(std::make_shared<ExceptColumnTransformerNode>(std::move(except_column_names), except_transformer->is_strict));
            }
        }
        else if (auto * replace_transformer = child->as<ASTColumnsReplaceTransformer>())
        {
            std::vector<ReplaceColumnTransformerNode::Replacement> replacements;
            replacements.reserve(replace_transformer->children.size());

            for (const auto & replace_transformer_child : replace_transformer->children)
            {
                auto & replacement = replace_transformer_child->as<ASTColumnsReplaceTransformer::Replacement &>();
                replacements.emplace_back(ReplaceColumnTransformerNode::Replacement{replacement.name, buildExpression(replacement.children[0], context)});
            }

            column_transformers.emplace_back(std::make_shared<ReplaceColumnTransformerNode>(replacements, replace_transformer->is_strict));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Unsupported column matcher {}", child->formatForErrorMessage());
        }
    }

    return column_transformers;
}

}

QueryTreeNodePtr buildQueryTree(ASTPtr query, ContextPtr context)
{
    QueryTreeBuilder builder;
    return builder.buildQueryTreeNode(std::move(query), context);
}

std::shared_ptr<TableFunctionNode> buildTableFunctionQueryTree(ASTPtr query, ContextPtr context)
{
    QueryTreeBuilder builder;
    return builder.buildTableFunction(query, context);
}

QueryTreeNodePtr QueryTreeBuilder::setSecondArgumentAsParameter(const ASTFunction * function, const ContextPtr & context) const
{
    const auto * second_arg_ast = function->arguments->children[1].get();
    const auto * second_arg_literal = second_arg_ast->as<ASTLiteral>();

    if (!second_arg_literal)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "If groupConcat is used with two arguments, the second argument must be a constant String");
    }

    if (second_arg_literal->value.getType() != Field::Types::String)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "If groupConcat is used with two arguments, the second argument must be a constant String");
    }

    ASTPtr first_arg = function->arguments->children[0]->clone();

    auto function_node = std::make_shared<FunctionNode>(function->name);
    function_node->setNullsAction(function->getNullsAction());

    /// Keep existing parameters (the optional limit)
    ///  the second argument overrides the delimiter at slot 0
    auto & parameters = function_node->getParameters().getNodes();
    if (function->parameters)
    {
        const auto & function_parameters_list = function->parameters->as<ASTExpressionList>()->children;
        for (const auto & parameter : function_parameters_list)
            parameters.push_back(buildExpression(parameter, context));
    }

    auto delimiter_node = buildExpression(function->arguments->children[1], context); // Separator
    if (parameters.empty())
        parameters.push_back(std::move(delimiter_node));
    else
        parameters[0] = std::move(delimiter_node);

    function_node->getArguments().getNodes().push_back(buildExpression(first_arg, context)); // Column to concatenate

    if (function->isWindowFunction())
    {
        if (function->window_definition)
            function_node->getWindowNode() = buildWindow(function->window_definition, context);
        else
            function_node->getWindowNode() = std::make_shared<IdentifierNode>(Identifier(function->window_name));
    }

    return std::move(function_node);
}

}
