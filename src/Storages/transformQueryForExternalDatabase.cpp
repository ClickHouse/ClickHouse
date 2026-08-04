#include <Common/typeid_cast.h>
#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <cmath>
#include <queue>


namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_strict_query;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Everything except numbers is put as string literal.
class ReplacingConstantExpressionsMatcherNumOrStr
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            const auto & result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            if (result.column->isNullAt(0))
            {
                node = make_intrusive<ASTLiteral>(Field());
            }
            else if (isNumber(result.type))
            {
                node = make_intrusive<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
            }
            else
            {
                /// Everything except numbers is put as string literal. This is important for Date, DateTime, UUID.

                const IColumn & inner_column = assert_cast<const ColumnConst &>(*result.column).getDataColumn();

                WriteBufferFromOwnString out;
                result.type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = make_intrusive<ASTLiteral>(out.str());
            }
        }
    }
};

struct ReplaceLiteralToExprVisitorData
{
    using TypeToVisit = ASTFunction;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (func.name == "and" || func.name == "or")
        {
            for (auto & argument : func.arguments->children)
            {
                auto * literal_expr = typeid_cast<ASTLiteral *>(argument.get());
                UInt64 value = 0;
                if (literal_expr && literal_expr->value.tryGet<UInt64>(value) && (value == 0 || value == 1))
                {
                    /// 1 -> 1=1, 0 -> 1=0.
                    if (value)
                        argument = makeASTOperator("equals", make_intrusive<ASTLiteral>(1), make_intrusive<ASTLiteral>(1));
                    else
                        argument = makeASTOperator("equals", make_intrusive<ASTLiteral>(1), make_intrusive<ASTLiteral>(0));
                }
            }
        }
    }
};

using ReplaceLiteralToExprVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplaceLiteralToExprVisitorData>, true>;

class DropAliasesMatcher
{
public:
    struct Data {};
    Data data;

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & node, Data)
    {
        if (!node->tryGetAlias().empty())
            node->setAlias({});
    }
};

void replaceConstantExpressions(ASTPtr & node, ContextPtr context, const NamesAndTypesList & all_columns)
{
    auto syntax_result = TreeRewriter(context).analyze(node, all_columns);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcherNumOrStr, true> visitor(block_with_constants);
    visitor.visit(node);
}

void dropAliases(ASTPtr & node)
{
    DropAliasesMatcher::Data data;
    InDepthNodeVisitor<DropAliasesMatcher, true> visitor(data);
    visitor.visit(node);
}


/// Returns true if `node` references a column of UUID type, including when that reference
/// is nested inside a tuple or another expression (e.g. the row comparison
/// `(uuid_col, x) < (...)`). ClickHouse and external databases (e.g. PostgreSQL) sort UUIDs
/// differently, so range comparisons involving a UUID column cannot be pushed down without
/// silently dropping rows.
bool containsUUIDColumn(const ASTPtr & node, const NamesAndTypesList & available_columns)
{
    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        for (const auto & column : available_columns)
            if (column.name == identifier->name())
                /// Unwrap LowCardinality / Nullable so e.g. LowCardinality(UUID) is recognised too.
                return WhichDataType(removeLowCardinalityAndNullable(column.type)).isUUID();
        return false;
    }

    for (const auto & child : node->children)
        if (containsUUIDColumn(child, available_columns))
            return true;

    return false;
}

/// Whether the field cannot be represented by the standard SQL literal formatter anywhere inside, including
/// nested inside a `Tuple` such as the right-hand side of `IN`.
bool fieldCannotBeRepresentedWithStandardSQLLiteralStyle(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::String:
            return field.safeGet<String>().find('\0') != String::npos;
        case Field::Types::Float64:
            return !std::isfinite(field.safeGet<Float64>());
        case Field::Types::Tuple:
        {
            for (const auto & element : field.safeGet<Tuple>())
                if (fieldCannotBeRepresentedWithStandardSQLLiteralStyle(element))
                    return true;
            return false;
        }
        case Field::Types::Array:
        {
            for (const auto & element : field.safeGet<Array>())
                if (fieldCannotBeRepresentedWithStandardSQLLiteralStyle(element))
                    return true;
            return false;
        }
        case Field::Types::Map:
        {
            for (const auto & element : field.safeGet<Map>())
                if (fieldCannotBeRepresentedWithStandardSQLLiteralStyle(element))
                    return true;
            return false;
        }
        default:
            return false;
    }
}

bool isCompatible(
    ASTPtr & node,
    const NamesAndTypesList & available_columns,
    LiteralEscapingStyle literal_escaping_style,
    const NameSet & unsupported_functions)
{
    if (auto * function = node->as<ASTFunction>())
    {
        if (function->parameters)   /// Parametric aggregate functions
            return false;

        if (!function->arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "function->arguments is not set");

        String name = function->name;

        if (unsupported_functions.contains(name))
            return false;

        if (!(name == "and"
            || name == "or"
            || name == "not"
            || name == "equals"
            || name == "notEquals"
            || name == "less"
            || name == "greater"
            || name == "lessOrEquals"
            || name == "greaterOrEquals"
            || name == "like"
            || name == "notLike"
            || name == "in"
            || name == "notIn"
            || name == "isNull"
            || name == "isNotNull"
            || name == "tuple"))
            return false;

        /// Range comparisons involving UUID columns must not be pushed down. ClickHouse and
        /// the external database sort UUIDs differently, so the pushed-down predicate compares
        /// against a different ordering and silently drops rows. This also covers tuple/row
        /// comparisons such as `(uuid_col, x) < (...)`, where the UUID column is nested inside
        /// a tuple. Equality and IN are order independent and remain compatible. Such predicates
        /// are applied locally instead. See https://github.com/ClickHouse/ClickHouse/issues/105558.
        if (name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals")
        {
            for (const auto & argument : function->arguments->children)
                if (containsUUIDColumn(argument, available_columns))
                    return false;
        }

        /// A tuple with zero or one elements is represented by a function tuple(x) and is not compatible,
        /// but a normal tuple with more than one element is represented as a parenthesized expression (x, y) and is perfectly compatible.
        /// So to support tuple with zero or one elements we can clear function name to get (x) instead of tuple(x)
        if (name == "tuple")
        {
            if (function->arguments->children.size() <= 1)
            {
                function->name.clear();
            }
        }

        /// If the right hand side of IN is a table identifier (example: x IN table), then it's not compatible.
        if ((name == "in" || name == "notIn")
            && (function->arguments->children.size() != 2 || function->arguments->children[1]->as<ASTTableIdentifier>()))
            return false;

        for (auto & expr : function->arguments->children)
            if (!isCompatible(expr, available_columns, literal_escaping_style, unsupported_functions))
                return false;

        /// When the parser's fast-path literal conversion produces
        /// `ASTLiteral(Tuple)` as the IN set (e.g. `(id, name) IN ((1, 'a'))`
        /// parsed as `in(tuple(id, name), ASTLiteral(Tuple{1, 'a'}))`),
        /// we must wrap it in a function with empty name so that it
        /// formats with an extra pair of parentheses: `((1, 'a'))`.
        /// Without this, `ASTLiteral(Tuple)` formats as `(1, 'a')` and the
        /// IN clause becomes `IN (1, 'a')` — which MySQL misinterprets
        /// as two separate scalar values instead of one tuple.
        ///
        /// We only do this when:
        /// 1. The LHS of IN is a multi-column tuple (`ASTFunction("tuple")`).
        ///    For scalar IN like `id IN (1, 2)`, the `ASTLiteral(Tuple{1, 2})`
        ///    is a flat list of values and must NOT be wrapped.
        /// 2. The tuple literal represents a single row (its elements are
        ///    plain values, not nested tuples). For multi-row sets like
        ///    `(id, name) IN ((1, 'a'), (2, 'b'))` the literal is
        ///    `Tuple{Tuple{1, 'a'}, Tuple{2, 'b'}}` which already formats
        ///    with the correct nested parentheses.
        if ((name == "in" || name == "notIn") && function->arguments->children.size() == 2)
        {
            const auto & lhs = function->arguments->children[0];
            const auto * lhs_func = lhs->as<ASTFunction>();
            bool lhs_is_tuple = lhs_func && lhs_func->name == "tuple";

            if (lhs_is_tuple)
            {
                auto & rhs = function->arguments->children[1];
                if (const auto * rhs_literal = rhs->as<ASTLiteral>())
                {
                    if (rhs_literal->value.getType() == Field::Types::Tuple)
                    {
                        const auto & tup = rhs_literal->value.safeGet<Tuple>();
                        bool is_single_row = !tup.empty()
                            && tup[0].getType() != Field::Types::Tuple;
                        if (is_single_row)
                            rhs = makeASTFunction("", rhs);
                    }
                }
            }
        }

        /// It should be formatted in the operator form.
        function->setIsOperator(true);

        return true;
    }

    if (const auto * literal = node->as<ASTLiteral>())
    {
        /// A standard SQL string literal cannot represent a NUL byte (SQLite receives NUL-terminated statement
        /// text), and the formatter emits non-finite floats as bare `inf`/`nan`, which SQLite parses as
        /// identifiers. Keep predicates containing either kind of literal local instead of pushing them down.
        if (literal_escaping_style == LiteralEscapingStyle::StandardSQL
            && fieldCannotBeRepresentedWithStandardSQLLiteralStyle(literal->value))
            return false;

        if (literal->value.getType() == Field::Types::Tuple)
        {
            /// Represent a tuple with zero or one elements as (x) instead of tuple(x).
            auto tuple_value = literal->value.safeGet<Tuple>();
            if (tuple_value.size() == 1)
            {
                node = makeASTFunction("", make_intrusive<ASTLiteral>(tuple_value[0]));
                return true;
            }
        }
        /// Foreign databases often have no support for Array. But Tuple literals are passed to support IN clause.
        return literal->value.getType() != Field::Types::Array;
    }

    return node->as<ASTIdentifier>();
}

bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names, const NameSet & local_only_names, bool & mentions_local_only);

void removeUnknownChildren(ASTs & children, const NameSet & known_names, const NameSet & local_only_names, bool & mentions_local_only)
{

    ASTs new_children;
    for (auto & child : children)
    {
        bool leave_child = removeUnknownSubexpressions(child, known_names, local_only_names, mentions_local_only);
        if (leave_child)
            new_children.push_back(child);
    }
    children = std::move(new_children);
}

/// return `true` if we should leave node in tree
bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names, const NameSet & local_only_names, bool & mentions_local_only)
{
    if (const auto * ident = node->as<ASTIdentifier>())
    {
        if (local_only_names.contains(ident->name()))
        {
            mentions_local_only = true;
            return false;
        }
        return known_names.contains(ident->name());
    }

    if (node->as<ASTLiteral>() != nullptr)
        return true;

    auto * func = node->as<ASTFunction>();
    if (func && (func->name == "and" || func->name == "or"))
    {
        /// Removing a conjunct only widens the remote filter (the removed condition is re-checked by the
        /// local filtering over the rows the external database returns), but removing a disjunct narrows
        /// it: a row matching only the removed branch is dropped remotely and never reaches the local
        /// re-filtering. So whenever any branch of a disjunction is removed — whether it mentions a column
        /// of another table of the query or a column that is present but not pushdown-safe
        /// (`local_only_names`) — the whole disjunction must stay local so the local filter can evaluate
        /// every branch over unfiltered rows.
        bool child_mentions_local_only = false;
        const size_t children_before = func->arguments->children.size();
        removeUnknownChildren(func->arguments->children, known_names, local_only_names, child_mentions_local_only);
        if (child_mentions_local_only)
            mentions_local_only = true;
        if (func->name == "or" && func->arguments->children.size() != children_before)
            return false;
        /// all children removed, current node can be removed too
        if (func->arguments->children.size() == 1)
        {
            /// if only one child left, pull it on top level
            node = func->arguments->children[0];
            return true;
        }
        return !func->arguments->children.empty();
    }

    bool leave_child = true;
    for (auto & child : node->children)
    {
        /// Visit every child even after the node is already known to be removed: a later child may mention
        /// a local-only column, and an enclosing disjunction must learn about it.
        leave_child = removeUnknownSubexpressions(child, known_names, local_only_names, mentions_local_only) && leave_child;
    }
    return leave_child;
}

// When a query references an external table such as table from MySQL database,
// the corresponding table storage has to execute the relevant part of the query. We
// send the query to the storage as AST. Before that, we have to remove the conditions
// that reference other tables from `WHERE`, so that the external engine is not confused
// by the unknown columns.
//
// `local_only_columns` are columns that do exist in the external table but whose predicates the caller
// requires to be evaluated locally (e.g. a pushdown over them would compare differently on the remote
// side). Their conditions are removed from the remote filter like unknown ones. A disjunction with any
// removed branch is removed as a whole (a narrower remote filter would lose rows), and
// `mentions_local_only` reports that some condition was kept local because of a local-only column (so
// strict mode can reject the query).
bool removeUnknownSubexpressionsFromWhere(ASTPtr & node, const NamesAndTypesList & available_columns, const NameSet & local_only_columns, bool & mentions_local_only)
{
    if (!node)
        return false;

    NameSet known_names;
    for (const auto & col : available_columns)
        known_names.insert(col.name);

    if (auto * expr_list = node->as<ASTExpressionList>(); expr_list && !expr_list->children.empty())
    {
        /// traverse expression list on top level
        removeUnknownChildren(expr_list->children, known_names, local_only_columns, mentions_local_only);
        return !expr_list->children.empty();
    }
    return removeUnknownSubexpressions(node, known_names, local_only_columns, mentions_local_only);
}

String transformQueryForExternalDatabaseImpl(
    ASTPtr clone_query,
    Names used_columns,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style,
    const String & database,
    const String & table,
    ContextPtr context,
    std::optional<size_t> limit,
    const NameSet & unsupported_functions,
    const NameSet & local_only_columns)
{
    bool strict = context->getSettingsRef()[Setting::external_table_strict_query];

    auto select = make_intrusive<ASTSelectQuery>();

    select->replaceDatabaseAndTable(database, table);

    auto select_expr_list = make_intrusive<ASTExpressionList>();
    for (const auto & name : used_columns)
        select_expr_list->children.push_back(make_intrusive<ASTIdentifier>(name));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    /** If there was WHERE,
      * copy it to transformed query if it is compatible,
      * or if it is AND expression,
      * copy only compatible parts of it.
      */

    ASTPtr original_where = clone_query->as<ASTSelectQuery &>().where();
    bool where_mentions_local_only = false;
    bool where_has_known_columns = removeUnknownSubexpressionsFromWhere(original_where, available_columns, local_only_columns, where_mentions_local_only);

    /// A condition kept local because it mentions a column the caller marked as not pushdown-safe is a
    /// condition the external database cannot evaluate, which is exactly what strict mode forbids.
    if (strict && where_mentions_local_only)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Query contains expressions over columns that cannot be evaluated by the external database "
                        "(and external_table_strict_query=true)");

    if (original_where && where_has_known_columns)
    {
        replaceConstantExpressions(original_where, context, available_columns);

        /// Replace like WHERE 1 AND 1 to WHERE 1 = 1 AND 1 = 1
        ReplaceLiteralToExprVisitor::Data replace_literal_to_expr_data;
        ReplaceLiteralToExprVisitor(replace_literal_to_expr_data).visit(original_where);

        if (isCompatible(original_where, available_columns, literal_escaping_style, unsupported_functions))
        {
            select->setExpression(ASTSelectQuery::Expression::WHERE, ASTPtr(original_where));
        }
        else if (strict)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Query contains non-compatible expressions (and external_table_strict_query=true)");
        }
        else if (auto * function = original_where->as<ASTFunction>())
        {
            if (function->name == "and" || function->name == "tuple")
            {
                auto new_function_and = makeASTOperator("and");
                std::queue<const ASTFunction *> predicates;
                predicates.push(function);

                while (!predicates.empty())
                {
                    const auto * func = predicates.front();
                    predicates.pop();

                    for (auto & elem : func->arguments->children)
                    {
                        if (isCompatible(elem, available_columns, literal_escaping_style, unsupported_functions))
                            new_function_and->arguments->children.push_back(elem);
                        else if (const auto * child = elem->as<ASTFunction>(); child && (child->name == "and" || child->name == "tuple"))
                            predicates.push(child);
                    }
                }

                if (new_function_and->arguments->children.size() == 1)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and->arguments->children[0]));
                else if (new_function_and->arguments->children.size() > 1)
                    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(new_function_and));
            }
        }
    }
    else if (strict && original_where)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Query contains non-compatible expressions '{}' (and external_table_strict_query=true)",
                        original_where->formatForErrorMessage());
    }

    auto * literal_expr = typeid_cast<ASTLiteral *>(original_where.get());
    UInt64 value = 0;
    if (literal_expr && literal_expr->value.tryGet<UInt64>(value) && (value == 0 || value == 1))
    {
        /// WHERE 1 -> WHERE 1=1, WHERE 0 -> WHERE 1=0.
        if (value)
            original_where = makeASTOperator("equals", make_intrusive<ASTLiteral>(1), make_intrusive<ASTLiteral>(1));
        else
            original_where = makeASTOperator("equals", make_intrusive<ASTLiteral>(1), make_intrusive<ASTLiteral>(0));
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(original_where));
    }

    if (limit)
        select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(*limit));

    ASTPtr select_ptr = select;
    dropAliases(select_ptr);
    IdentifierQuotingRule identifier_quoting_rule = IdentifierQuotingRule::Always;
    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(
        /*one_line=*/true,
        /*identifier_quoting_rule=*/identifier_quoting_rule,
        /*identifier_quoting_style=*/identifier_quoting_style,
        /*show_secrets_=*/true,
        /*literal_escaping_style=*/literal_escaping_style);

    select->format(out, settings);

    return out.str();
}

}

String transformQueryForExternalDatabase(
    const SelectQueryInfo & query_info,
    const Names & column_names,
    const NamesAndTypesList & available_columns,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style,
    const String & database,
    const String & table,
    ContextPtr context,
    std::optional<size_t> limit,
    const NameSet & unsupported_functions,
    const NameSet & local_only_columns)
{
    if (!query_info.syntax_analyzer_result)
    {
        if (!query_info.query_tree)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Query is not analyzed: no query tree");
        if (!query_info.planner_context)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Query is not analyzed: no planner context");
        if (!query_info.table_expression)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Query is not analyzed: no table expression");

        if (column_names.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "No column names for query '{}' to external table '{}.{}'",
                            query_info.query_tree->formatASTForErrorMessage(), database, table);

        auto clone_query = getASTForExternalDatabaseFromQueryTree(context, query_info.query_tree, query_info.table_expression);

        return transformQueryForExternalDatabaseImpl(
            clone_query,
            column_names,
            available_columns,
            identifier_quoting_style,
            literal_escaping_style,
            database,
            table,
            context,
            limit,
            unsupported_functions,
            local_only_columns);
    }

    auto clone_query = query_info.query->clone();
    return transformQueryForExternalDatabaseImpl(
        clone_query,
        query_info.syntax_analyzer_result->requiredSourceColumns(),
        available_columns,
        identifier_quoting_style,
        literal_escaping_style,
        database,
        table,
        context,
        limit,
        unsupported_functions,
        local_only_columns);
}

void rejectOuterFilterForQueryBackedExternalSourceIfStrict(const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::external_table_strict_query])
        return;

    /// Reconstruct the outer query the same way `transformQueryForExternalDatabase` does, and check whether it
    /// carries a filter on the source. For a query-backed source the user's query is passed to the external
    /// database verbatim, so such a filter could only be applied locally - which `external_table_strict_query`
    /// forbids.
    ASTPtr clone_query;
    if (!query_info.syntax_analyzer_result)
    {
        /// The analyzer has not produced an AST yet; nothing to inspect if the query tree is unavailable.
        if (!query_info.query_tree || !query_info.table_expression)
            return;
        clone_query = getASTForExternalDatabaseFromQueryTree(context, query_info.query_tree, query_info.table_expression);
    }
    else if (query_info.query)
    {
        clone_query = query_info.query->clone();
    }
    else
        return;

    const auto * select = clone_query->as<ASTSelectQuery>();
    if (select && (select->where() || select->prewhere()))
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The query contains a filter that cannot be pushed down to the external database, because the data "
            "source is a query passed to it as is (and external_table_strict_query=true). Move the filter inside "
            "the passed query, or disable external_table_strict_query.");
}

}
