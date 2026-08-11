#include <Common/checkStackSize.h>
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

#include <queue>


namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_strict_query;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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


/// SQLite has no way to represent a NUL byte inside a string literal (see writeQuotedStringSQLite),
/// and a PostgreSQL string value cannot contain a NUL byte at all (see
/// writeQuotedStringPostgreSQLLossless), so a predicate whose string literal (possibly nested in
/// an IN tuple / array / map) contains NUL must not be pushed down to those databases; it is
/// evaluated by ClickHouse instead.
bool fieldHasStringWithNulByte(const Field & field)
{
    checkStackSize();

    switch (field.getType())
    {
        case Field::Types::String:
            return field.safeGet<String>().contains('\0');
        case Field::Types::Tuple:
            for (const auto & element : field.safeGet<Tuple>())
                if (fieldHasStringWithNulByte(element))
                    return true;
            return false;
        case Field::Types::Array:
            for (const auto & element : field.safeGet<Array>())
                if (fieldHasStringWithNulByte(element))
                    return true;
            return false;
        case Field::Types::Map:
            for (const auto & element : field.safeGet<Map>())
                if (fieldHasStringWithNulByte(element))
                    return true;
            return false;
        default:
            return false;
    }
}

/// External databases have no ClickHouse-compatible syntax for `Array` / `Map` literals: the
/// dialect formatters (see `FieldVisitorToStringForDialect` in `ASTLiteral.cpp`) would emit them
/// with ClickHouse `[...]` syntax, which the external database rejects. A top-level `Array` is
/// rejected in `isCompatible` directly, but such values can also hide inside `IN` tuples, e.g.
/// `(id, arr_col) IN ((1, [1, 2]))`, so the check has to recurse. `Tuple` itself stays allowed:
/// it serializes as a parenthesized list, which external databases understand in `IN` sets.
bool fieldContainsArrayOrMap(const Field & field)
{
    checkStackSize();

    switch (field.getType())
    {
        case Field::Types::Array:
        case Field::Types::Map:
            return true;
        case Field::Types::Tuple:
            for (const auto & element : field.safeGet<Tuple>())
                if (fieldContainsArrayOrMap(element))
                    return true;
            return false;
        default:
            return false;
    }
}

/// Returns true if the field can only be written back in ClickHouse-specific syntax that an
/// external database cannot parse: an `Array` / `Map` at any depth, or a tuple with fewer than
/// two elements (which has no plain parenthesized form and can only be written as `tuple(...)`).
/// Mirrors what `FieldVisitorToStringForDialect` rejects at format time for the PostgreSQL /
/// SQLite escaping styles, for use where the `Regular` style (MySQL) is formatted instead.
bool fieldRequiresClickHouseOnlySyntax(const Field & field)
{
    checkStackSize();

    switch (field.getType())
    {
        case Field::Types::Array:
        case Field::Types::Map:
            return true;
        case Field::Types::Tuple:
        {
            const auto & tuple = field.safeGet<Tuple>();
            if (tuple.size() < 2)
                return true;
            for (const auto & element : tuple)
                if (fieldRequiresClickHouseOnlySyntax(element))
                    return true;
            return false;
        }
        default:
            return false;
    }
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

/// Returns true if the AST node is a tuple: either an `ASTLiteral` holding a `Tuple` field
/// (the parser's fast-path literal conversion) or an explicit `tuple(...)` function call.
bool isTupleNode(const ASTPtr & node)
{
    if (const auto * literal = node->as<ASTLiteral>())
        return literal->value.getType() == Field::Types::Tuple;
    if (const auto * function = node->as<ASTFunction>())
        return function->name == "tuple";
    return false;
}

/// A single-row multi-column IN set (e.g. `(id, name) IN ((1, 'a'))`) must keep its outer
/// parentheses when re-serialized for the external database: without them the IN clause becomes
/// `IN (1, 'a')` — which external databases misinterpret as two separate scalar values instead of
/// one tuple. Such a set arrives in one of two carriers, and we wrap either of them in a function
/// with empty name so it formats with an extra pair of parentheses, `((1, 'a'))`:
/// - the parser's fast-path literal conversion, `in(tuple(id, name), ASTLiteral(Tuple{1, 'a'}))`,
///   whose `ASTLiteral(Tuple)` formats as `(1, 'a')`;
/// - an explicit function call, `(id, name) IN (tuple(1, 'a'))`, whose `tuple` is later switched
///   to the operator form and also formats as `(1, 'a')`.
///
/// We only do this when:
/// 1. The LHS of IN is a multi-column tuple (`ASTFunction("tuple")`). For scalar IN like
///    `id IN (1, 2)`, the `ASTLiteral(Tuple{1, 2})` is a flat list of values and must NOT be wrapped.
/// 2. The tuple represents a single row (its elements are plain values, not nested tuples).
///    For multi-row sets like `(id, name) IN ((1, 'a'), (2, 'b'))` the set is a tuple of tuples
///    which already formats with the correct nested parentheses.
void wrapSingleRowTupleSetForINNode(ASTFunction & function)
{
    if (!(function.name == "in" || function.name == "notIn")
        || !function.arguments || function.arguments->children.size() != 2)
        return;

    const auto * lhs_func = function.arguments->children[0]->as<ASTFunction>();
    if (!lhs_func || lhs_func->name != "tuple")
        return;

    auto & rhs = function.arguments->children[1];

    if (const auto * rhs_literal = rhs->as<ASTLiteral>())
    {
        if (rhs_literal->value.getType() != Field::Types::Tuple)
            return;

        const auto & tup = rhs_literal->value.safeGet<Tuple>();
        if (!tup.empty() && tup[0].getType() != Field::Types::Tuple)
            rhs = makeASTFunction("", rhs);
        return;
    }

    if (const auto * rhs_func = rhs->as<ASTFunction>();
        rhs_func && rhs_func->name == "tuple" && rhs_func->arguments)
    {
        /// The user's own parentheses around the RHS (`IN (tuple(...))`) set the `parenthesized`
        /// flag on the `tuple` node; together with the parentheses of the operator form of `tuple`
        /// (or of the wrapper added below) they would produce a redundant layer, `IN (((1, 'a')))`.
        rhs->setParenthesized(false);

        const auto & elements = rhs_func->arguments->children;
        if (!elements.empty() && !isTupleNode(elements[0]))
            rhs = makeASTFunction("", rhs);
    }
}

/// Where a tuple may legally appear as the row value `(a, b)` in the SQL of the external
/// database. SQLite and MySQL accept row values only next to a comparison or `IN`
/// (`(a, b) = (1, 'x')`, `(a, b) IN ((1, 'x'), (2, 'y'))`); anywhere else - e.g. in the SELECT
/// list or as the argument of `IS NOT NULL` - the parenthesized form is a syntax error there
/// (SQLite reports "row value misused"), so such tuples must be rejected in ClickHouse instead
/// of being sent as broken SQL.
enum class RowValueContext
{
    /// A tuple here has no valid text form for the external database.
    Disallowed,
    /// A boolean position: the whole pushed-down `WHERE`, or an operand of `AND` / `OR` / `NOT`.
    /// No external database accepts a row value as a condition (PostgreSQL: "argument of WHERE
    /// must be type boolean, not type record"), not even the ones that accept row values as
    /// ordinary value expressions.
    BooleanPredicate,
    /// A tuple here is a row value: an operand of a comparison, the left-hand side of `IN`,
    /// or an element of an `IN` set.
    RowValue,
    /// The node is the right-hand side of `IN`: a tuple here is the `IN` set itself, and its
    /// elements may in turn be row values (the multi-row case).
    INSet,
};

/// In PostgreSQL a row constructor is an ordinary value expression, valid in any expression
/// position (`SELECT (a, b)`, `WHERE (a, b) IS NOT NULL`), so the row-value position
/// restriction above does not apply to it.
bool rowValueIsValidAnywhere(LiteralEscapingStyle literal_escaping_style)
{
    return literal_escaping_style == LiteralEscapingStyle::PostgreSQL;
}

/// Whether the external database accepts the row value `(a, b)` in the position described by
/// `row_value_context`. A tuple in a `BooleanPredicate` position is never a row value for the
/// external database: it is ClickHouse's list-of-predicates form, which the callers lower to a
/// conjunction instead (the pushdown path splits it into separate predicates after this check
/// fails, `normalizeSubqueryForExternalDatabaseImpl` rewrites the `tuple` call to `and` before
/// reaching it); the folded `Tuple` literal carrier of constants stays rejected.
bool rowValueIsAllowedHere(RowValueContext row_value_context, LiteralEscapingStyle literal_escaping_style)
{
    switch (row_value_context)
    {
        case RowValueContext::RowValue:
        case RowValueContext::INSet:
            return true;
        case RowValueContext::BooleanPredicate:
            return false;
        case RowValueContext::Disallowed:
            return rowValueIsValidAnywhere(literal_escaping_style);
    }
}

/// The position of an argument of `function` (the `index`-th one) with respect to row values:
/// the operands of a comparison and both sides of `IN` are the only places where the external
/// database accepts one. The elements of a row are plain values, while the elements of an `IN`
/// set are rows themselves (the multi-row case).
RowValueContext argumentRowValueContext(const String & function_name, size_t index, RowValueContext function_context)
{
    if (function_name == "and" || function_name == "or" || function_name == "not")
        return RowValueContext::BooleanPredicate;

    if (function_name == "in" || function_name == "notIn")
        return index == 0 ? RowValueContext::RowValue : RowValueContext::INSet;

    if (function_name == "equals" || function_name == "notEquals"
        || function_name == "less" || function_name == "greater"
        || function_name == "lessOrEquals" || function_name == "greaterOrEquals"
        || function_name == "isNotDistinctFrom")
        return RowValueContext::RowValue;

    if (function_name == "tuple")
        return function_context == RowValueContext::INSet ? RowValueContext::RowValue : RowValueContext::Disallowed;

    return RowValueContext::Disallowed;
}

bool isCompatible(ASTPtr & node, LiteralEscapingStyle literal_escaping_style, const NamesAndTypesList & available_columns, RowValueContext row_value_context)
{
    /// The AST comes from a user query, so its depth is unbounded - fail with `TOO_DEEP_RECURSION`
    /// instead of exhausting the stack.
    checkStackSize();

    if (auto * function = node->as<ASTFunction>())
    {
        if (function->parameters)   /// Parametric aggregate functions
            return false;

        if (!function->arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "function->arguments is not set");

        String name = function->name;

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
            /// A multi-column tuple is written as the row value `(a, b)`, which most external
            /// databases accept only next to a comparison or `IN` (see `RowValueContext`).
            /// Elsewhere - e.g. `WHERE (a, b) IS NOT NULL` - the predicate has no valid form for
            /// the external database, so it must not be pushed down and is applied locally.
            else if (!rowValueIsAllowedHere(row_value_context, literal_escaping_style))
                return false;
        }

        /// If the right hand side of IN is a table identifier (example: x IN table), then it's not compatible.
        if ((name == "in" || name == "notIn")
            && (function->arguments->children.size() != 2 || function->arguments->children[1]->as<ASTTableIdentifier>()))
            return false;

        auto & arguments = function->arguments->children;
        for (size_t i = 0; i < arguments.size(); ++i)
            if (!isCompatible(arguments[i], literal_escaping_style, available_columns,
                              argumentRowValueContext(name, i, row_value_context)))
                return false;

        /// Normalize a single-row multi-column IN set so it does not collapse to scalars when
        /// re-serialized for the external database (see `wrapSingleRowTupleSetForINNode`).
        wrapSingleRowTupleSetForINNode(*function);

        /// It should be formatted in the operator form.
        function->setIsOperator(true);

        return true;
    }

    if (const auto * literal = node->as<ASTLiteral>())
    {
        /// SQLite cannot represent NUL bytes in string literals, and PostgreSQL cannot store NUL
        /// bytes in string values, so do not push such predicates down.
        if (literal_escaping_style != LiteralEscapingStyle::Regular && fieldHasStringWithNulByte(literal->value))
            return false;

        /// Foreign databases often have no support for Array / Map, and ClickHouse would serialize
        /// such literals with its own `[...]` syntax anyway (see `fieldContainsArrayOrMap`); this
        /// also covers Array / Map nested inside IN tuples (including single-element tuples, which
        /// are unwrapped below). Tuple literals themselves are passed to support the IN clause.
        if (fieldContainsArrayOrMap(literal->value))
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
            /// The parser folds `(1, 'x')` into a single `ASTLiteral` holding a `Tuple` field,
            /// which formats as the row value `(1, 'x')` and is therefore subject to the same
            /// position restriction as the `tuple` function above.
            if (!rowValueIsAllowedHere(row_value_context, literal_escaping_style))
                return false;
        }
        return true;
    }

    return node->as<ASTIdentifier>();
}

bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names);

void removeUnknownChildren(ASTs & children, const NameSet & known_names)
{

    ASTs new_children;
    for (auto & child : children)
    {
        bool leave_child = removeUnknownSubexpressions(child, known_names);
        if (leave_child)
            new_children.push_back(child);
    }
    children = std::move(new_children);
}

/// return `true` if we should leave node in tree
bool removeUnknownSubexpressions(ASTPtr & node, const NameSet & known_names)
{
    if (const auto * ident = node->as<ASTIdentifier>())
        return known_names.contains(ident->name());

    if (node->as<ASTLiteral>() != nullptr)
        return true;

    auto * func = node->as<ASTFunction>();
    if (func && (func->name == "and" || func->name == "or"))
    {
        removeUnknownChildren(func->arguments->children, known_names);
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
        leave_child = leave_child && removeUnknownSubexpressions(child, known_names);
        if (!leave_child)
            break;
    }
    return leave_child;
}

// When a query references an external table such as table from MySQL database,
// the corresponding table storage has to execute the relevant part of the query. We
// send the query to the storage as AST. Before that, we have to remove the conditions
// that reference other tables from `WHERE`, so that the external engine is not confused
// by the unknown columns.
bool removeUnknownSubexpressionsFromWhere(ASTPtr & node, const NamesAndTypesList & available_columns)
{
    if (!node)
        return false;

    NameSet known_names;
    for (const auto & col : available_columns)
        known_names.insert(col.name);

    if (auto * expr_list = node->as<ASTExpressionList>(); expr_list && !expr_list->children.empty())
    {
        /// traverse expression list on top level
        removeUnknownChildren(expr_list->children, known_names);
        return !expr_list->children.empty();
    }
    return removeUnknownSubexpressions(node, known_names);
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
    std::optional<size_t> limit)
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
    bool where_has_known_columns = removeUnknownSubexpressionsFromWhere(original_where, available_columns);

    if (original_where && where_has_known_columns)
    {
        replaceConstantExpressions(original_where, context, available_columns);

        /// Replace like WHERE 1 AND 1 to WHERE 1 = 1 AND 1 = 1
        ReplaceLiteralToExprVisitor::Data replace_literal_to_expr_data;
        ReplaceLiteralToExprVisitor(replace_literal_to_expr_data).visit(original_where);

        if (isCompatible(original_where, literal_escaping_style, available_columns, RowValueContext::BooleanPredicate))
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
                        if (isCompatible(elem, literal_escaping_style, available_columns, RowValueContext::BooleanPredicate))
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
    std::optional<size_t> limit)
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
            limit);
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
        limit);
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

static void normalizeSubqueryForExternalDatabaseImpl(ASTPtr & node, LiteralEscapingStyle literal_escaping_style, RowValueContext row_value_context)
{
    if (!node)
        return;
    /// The AST comes from a user query, so its depth is unbounded - fail with `TOO_DEEP_RECURSION`
    /// instead of exhausting the stack.
    checkStackSize();
    if (auto * function = node->as<ASTFunction>())
    {
        /// When the analyzer re-serializes the subquery argument from its query tree,
        /// `ConstantNode::toAST` wraps a literal whose inferred type does not survive the text
        /// round trip in an internal `_CAST` call: `(2, 'y')` comes back as
        /// `_CAST((2, 'y'), 'Tuple(UInt8, String)')`. `_CAST` is ClickHouse-internal and must not
        /// reach the external database - unwrap it back to the literal, which is exactly what the
        /// user wrote (the type annotation is meaningless for the external database anyway).
        if (function->name == "_CAST" && function->arguments && function->arguments->children.size() == 2
            && function->arguments->children[0]->as<ASTLiteral>())
        {
            const auto * type_literal = function->arguments->children[1]->as<ASTLiteral>();
            if (type_literal && type_literal->value.getType() == Field::Types::String)
            {
                node = function->arguments->children[0];
                normalizeSubqueryForExternalDatabaseImpl(node, literal_escaping_style, row_value_context);
                return;
            }
        }

        wrapSingleRowTupleSetForINNode(*function);

        /// `tuple` with at least two arguments has the plain parenthesized form `(a, b)` that
        /// MySQL / PostgreSQL / SQLite understand as a row value, but only when formatted as an
        /// operator and only in a row-value position; the function-call form `tuple(a, b)` is
        /// ClickHouse syntax everywhere. With fewer than two arguments (and for `array` / `map`)
        /// there is no such form at all, so the re-serialized subquery would not be parseable by
        /// the external database - reject it instead of sending broken SQL.
        if (function->name == "tuple")
        {
            /// In a boolean position a tuple is not a row value but ClickHouse's list-of-predicates
            /// form (`WHERE (a > 0, b > 10)`), and no external database accepts a row value as a
            /// condition anyway (PostgreSQL: "argument of WHERE must be type boolean, not type
            /// record"). Lower it to a conjunction, exactly as the predicate-pushdown path does.
            if (row_value_context == RowValueContext::BooleanPredicate
                && function->arguments && !function->arguments->children.empty())
            {
                if (function->arguments->children.size() == 1)
                {
                    node = function->arguments->children[0];
                }
                else
                {
                    function->name = "and";
                    /// `AND` only has the operator text form (`and(...)` is ClickHouse syntax).
                    function->setIsOperator(true);
                }
                normalizeSubqueryForExternalDatabaseImpl(node, literal_escaping_style, RowValueContext::BooleanPredicate);
                return;
            }

            if (!function->arguments || function->arguments->children.size() < 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot format a tuple with fewer than two elements for the external database: "
                    "it can only be written in ClickHouse-specific syntax. Rewrite the query passed "
                    "to the external database without it");
            if (!rowValueIsAllowedHere(row_value_context, literal_escaping_style))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot format a tuple for the external database: the row value ('x', 'y') is "
                    "only valid as an operand of a comparison or IN there. Rewrite the query passed "
                    "to the external database without it");
            function->setIsOperator(true);
            /// The elements of an `IN` set are rows; the elements of a row are plain values.
            auto element_context = argumentRowValueContext(function->name, 0, row_value_context);
            for (auto & child : function->arguments->children)
                normalizeSubqueryForExternalDatabaseImpl(child, literal_escaping_style, element_context);
            return;
        }

        if (function->name == "array" || function->name == "map")
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot format {} for the external database: it has no syntax for such values. "
                "Rewrite the query passed to the external database without it",
                function->name == "array" ? "an array" : "a map");
        }

        if ((function->name == "in" || function->name == "notIn")
            && function->arguments && function->arguments->children.size() == 2)
        {
            normalizeSubqueryForExternalDatabaseImpl(function->arguments->children[0], literal_escaping_style, RowValueContext::RowValue);
            normalizeSubqueryForExternalDatabaseImpl(function->arguments->children[1], literal_escaping_style, RowValueContext::INSet);
            return;
        }

        /// The operands of a comparison may be row values. `isNotDistinctFrom` is the NULL-safe
        /// equality `<=>`, which MySQL accepts for row values just like the other comparisons.
        if ((function->name == "equals" || function->name == "notEquals"
             || function->name == "less" || function->name == "greater"
             || function->name == "lessOrEquals" || function->name == "greaterOrEquals"
             || function->name == "isNotDistinctFrom")
            && function->arguments)
        {
            for (auto & child : function->arguments->children)
                normalizeSubqueryForExternalDatabaseImpl(child, literal_escaping_style, RowValueContext::RowValue);
            return;
        }

        /// The operands of `AND` / `OR` / `NOT` are boolean positions themselves - a row value is
        /// not accepted there by any external database, but a tuple there is ClickHouse's
        /// list-of-predicates form and is lowered to a conjunction (see the `tuple` branch above).
        if ((function->name == "and" || function->name == "or" || function->name == "not")
            && function->arguments)
        {
            for (auto & child : function->arguments->children)
                normalizeSubqueryForExternalDatabaseImpl(child, literal_escaping_style, RowValueContext::BooleanPredicate);
            return;
        }

        if (function->name.empty())
        {
            /// The parentheses-only wrapper added by `wrapSingleRowTupleSetForINNode` is
            /// transparent: the wrapped node keeps the position of the wrapper itself.
            if (function->arguments)
                for (auto & child : function->arguments->children)
                    normalizeSubqueryForExternalDatabaseImpl(child, literal_escaping_style, row_value_context);
            return;
        }
    }
    else if (const auto * literal = node->as<ASTLiteral>())
    {
        /// The literal carrier of a row value (the parser folds `(1, 'x')` into a single
        /// `ASTLiteral` holding a `Tuple` field) is subject to the same restriction as the
        /// `tuple` function above: outside a row-value position its parenthesized text form
        /// is a syntax error for the external database.
        if (!rowValueIsAllowedHere(row_value_context, literal_escaping_style)
            && literal->value.getType() == Field::Types::Tuple
            && literal->value.safeGet<Tuple>().size() >= 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot format a tuple for the external database: the row value ('x', 'y') is "
                "only valid as an operand of a comparison or IN there. Rewrite the query passed "
                "to the external database without it");
        /// For PostgreSQL / SQLite the dialect field visitors reject such literals at format time
        /// (see `FieldVisitorToStringForDialect`); the `Regular` style - used for MySQL, whose
        /// string literals interpret backslash escapes the same way as ClickHouse - formats them
        /// in ClickHouse-only syntax (`[...]`, `tuple(x)`) without complaint, so reject them here.
        if (literal_escaping_style == LiteralEscapingStyle::Regular
            && fieldRequiresClickHouseOnlySyntax(literal->value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot format a literal of type {} for the external database: it can only be "
                "written in ClickHouse-specific syntax. Rewrite the query passed to the external "
                "database without it",
                literal->value.getTypeName());
    }
    if (auto * select = node->as<ASTSelectQuery>())
    {
        /// `PREWHERE` is ClickHouse-only syntax that no external database can parse; for the
        /// external database it is an ordinary filter, so lower it into `WHERE`, merging with
        /// an existing `WHERE` via `AND`.
        if (select->prewhere())
        {
            ASTPtr filter = select->prewhere();
            select->setExpression(ASTSelectQuery::Expression::PREWHERE, {});
            if (auto where = select->where())
                filter = makeASTOperator("and", std::move(filter), std::move(where));
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(filter));
        }
        /// The filtering clauses of the subquery are boolean positions: a tuple there is
        /// ClickHouse's list-of-predicates form, not a row value (no external database accepts
        /// a row value as a condition).
        for (auto & child : node->children)
        {
            const bool is_boolean_clause = (select->where() && child == select->where())
                || (select->having() && child == select->having())
                || (select->qualify() && child == select->qualify());
            normalizeSubqueryForExternalDatabaseImpl(
                child, literal_escaping_style,
                is_boolean_clause ? RowValueContext::BooleanPredicate : RowValueContext::Disallowed);
        }
        return;
    }
    for (auto & child : node->children)
        normalizeSubqueryForExternalDatabaseImpl(child, literal_escaping_style, RowValueContext::Disallowed);
}

void normalizeSubqueryForExternalDatabase(ASTPtr & node, LiteralEscapingStyle literal_escaping_style)
{
    normalizeSubqueryForExternalDatabaseImpl(node, literal_escaping_style, RowValueContext::Disallowed);
}

}
