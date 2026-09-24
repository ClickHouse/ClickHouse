#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <fmt/ranges.h>

namespace DB
{

namespace
{

/// Returns `type` if it is a DateTime/DateTime64 carrying no explicit timezone. Returns nullptr otherwise.
DataTypePtr eligibleDateTimeType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(removeLowCardinality(type));

    if (const auto * dt = typeid_cast<const DataTypeDateTime *>(unwrapped.get()))
        return dt->hasExplicitTimeZone() ? nullptr : unwrapped;
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(unwrapped.get()))
        return dt64->hasExplicitTimeZone() ? nullptr : unwrapped;
    return nullptr;
}

/// True if the identifier's first name part is bound by an enclosing lambda: it is then that lambda's
/// parameter, not the storage column of the same name, so only the plain column reading applies to it.
bool isBoundByLambda(const ASTIdentifier & identifier, const std::vector<String> & lambda_parameters)
{
    const auto & parts = identifier.name_parts;
    return !parts.empty() && std::ranges::find(lambda_parameters, parts.front()) != lambda_parameters.end();
}

/// Returns the DateTime/DateTime64 column type if `column_name` refers to
/// a DateTime column without an explicit timezone. Returns nullptr otherwise.
DataTypePtr getDateTimeColumnType(const String & column_name, const ColumnsDescription & columns)
{
    const auto * desc = columns.tryGet(column_name);
    return desc ? eligibleDateTimeType(desc->type) : nullptr;
}

/// Same, for an identifier that may carry a table or database qualifier.
DataTypePtr getDateTimeColumnType(
    const ASTIdentifier & identifier,
    const ColumnsDescription & columns,
    const StorageID & table_id,
    bool bound_by_lambda = false)
{
    if (bound_by_lambda)
        return getDateTimeColumnType(identifier.name(), columns);

    const auto & parts = identifier.name_parts;

    /// Readings of the name, in analyzer order: the whole name, then this table's own name as a
    /// qualifier, then its database and table. The first reading that names a column or a
    /// subcolumn decides, and its own type is the answer - a name that already resolves must not
    /// have a qualifier dropped, or the literal would take a different column's timezone.
    std::vector<size_t> column_name_offsets{0};
    if (parts.size() > 1 && parts[0] == table_id.table_name)
        column_name_offsets.push_back(1);
    if (parts.size() > 2 && parts[0] == table_id.database_name && parts[1] == table_id.table_name)
        column_name_offsets.push_back(2);

    for (size_t offset : column_name_offsets)
    {
        const auto column_name = offset == 0
            ? identifier.name()
            : fmt::format("{}", fmt::join(parts.begin() + offset, parts.end(), "."));

        /// Subcolumns must be visible here: a `Tuple(time DateTime('UTC'))` element is not a real
        /// column, so an exact-name lookup would miss `x.time` and fall through to a same-named
        /// top-level column, wrapping the literal in the wrong timezone.
        if (auto resolved = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name))
            return eligibleDateTimeType(resolved->type);
    }
    return nullptr;
}

/// Wraps a string literal AST with toDateTime('...', 'tz') or toDateTime64('...', scale, 'tz').
ASTPtr wrapWithTimezone(const ASTPtr & literal_ast, const DataTypePtr & datetime_type, const String & timezone)
{
    auto tz_literal = make_intrusive<ASTLiteral>(Field(timezone));

    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(datetime_type.get()))
    {
        auto scale_literal = make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(dt64->getScale())));
        return makeASTFunction("toDateTime64", literal_ast, std::move(scale_literal), std::move(tz_literal));
    }

    return makeASTFunction("toDateTime", literal_ast, std::move(tz_literal));
}

/// For a comparison like `column >= 'datetime-string'`, try to wrap the string
/// literal with an explicit timezone cast. Modifies the AST in place.
/// Returns true if any literal was wrapped.
bool tryWrapComparisonLiteral(
    ASTFunction & function,
    const ColumnsDescription & columns,
    const StorageID & table_id,
    const String & timezone,
    const std::vector<String> & lambda_parameters)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    bool wrapped = false;
    auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    /// Check left=identifier, right=string-literal
    if (const auto * id = left->as<ASTIdentifier>())
    {
        if (auto dt = getDateTimeColumnType(*id, columns, table_id, isBoundByLambda(*id, lambda_parameters)))
        {
            if (const auto * lit = right->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
            {
                right = wrapWithTimezone(right, dt, timezone);
                wrapped = true;
            }
        }
    }

    /// Check right=identifier, left=string-literal (e.g. '2000-01-01' <= time)
    if (const auto * id = right->as<ASTIdentifier>())
    {
        if (auto dt = getDateTimeColumnType(*id, columns, table_id, isBoundByLambda(*id, lambda_parameters)))
        {
            if (const auto * lit = left->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
            {
                left = wrapWithTimezone(left, dt, timezone);
                wrapped = true;
            }
        }
    }

    return wrapped;
}

/// `datetime_type` is the column's own type, which carries no timezone.
DataTypePtr withExplicitTimezone(const DataTypePtr & datetime_type, const String & timezone)
{
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(datetime_type.get()))
        return std::make_shared<DataTypeDateTime64>(dt64->getScale(), timezone);
    return std::make_shared<DataTypeDateTime>(timezone);
}

/// The timezone is part of the target type, so the cast denotes the same instants wherever it is read.
ASTPtr castCollectionWithTimezone(Array elements, const DataTypePtr & datetime_type, const String & timezone)
{
    auto array_type = std::make_shared<DataTypeArray>(withExplicitTimezone(datetime_type, timezone));
    return makeASTFunction(
        "CAST",
        make_intrusive<ASTLiteral>(std::move(elements)),
        make_intrusive<ASTLiteral>(array_type->getName()));
}

/// Rewrites a folded literal collection so its string elements carry an explicit timezone.
/// A collection of only strings is cast as a whole, so the stored command and its query tree stay
/// O(1) in the element count; a mixed collection has no common element type and must be expanded.
/// Returns nullptr when there is no string element, leaving the original literal and its type alone.
template <typename Collection>
ASTPtr rewriteCollectionWithTimezone(
    const Collection & elements,
    std::string_view expanded_function_name,
    const DataTypePtr & datetime_type,
    const String & timezone)
{
    size_t strings = 0;
    for (const auto & element : elements)
        strings += element.getType() == Field::Types::String;

    if (strings == 0)
        return nullptr;

    if (strings == elements.size())
        return castCollectionWithTimezone(Array(elements.begin(), elements.end()), datetime_type, timezone);

    auto function = makeASTFunction(expanded_function_name);
    auto & arguments = function->arguments->children;
    arguments.reserve(elements.size());
    for (const auto & element : elements)
    {
        ASTPtr element_ast = make_intrusive<ASTLiteral>(element);
        if (element.getType() == Field::Types::String)
            element_ast = wrapWithTimezone(element_ast, datetime_type, timezone);
        arguments.push_back(std::move(element_ast));
    }
    return function;
}

/// For an IN expression like `column IN ('dt1', 'dt2')`, wrap each string literal.
/// Returns true if any literal was wrapped.
bool tryWrapInLiterals(
    ASTFunction & function,
    const ColumnsDescription & columns,
    const StorageID & table_id,
    const String & timezone,
    const std::vector<String> & lambda_parameters)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    const auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    const auto * id = left->as<ASTIdentifier>();
    if (!id)
        return false;

    const bool bound_by_lambda = isBoundByLambda(*id, lambda_parameters);
    auto dt = getDateTimeColumnType(*id, columns, table_id, bound_by_lambda);
    if (!dt)
        return false;

    bool wrapped = false;

    /// The right side of IN can be an ASTFunction (tuple) or ASTExpressionList.
    /// Walk its children and wrap string literals.
    auto wrap_children = [&](ASTs & children)
    {
        for (auto & child : children)
        {
            if (const auto * lit = child->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
            {
                child = wrapWithTimezone(child, dt, timezone);
                wrapped = true;
            }
        }
    };

    if (auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple" && tuple_func->arguments)
        wrap_children(tuple_func->arguments->children);
    else if (auto * expr_list = right->as<ASTExpressionList>())
        wrap_children(expr_list->children);
    /// The folded branch additionally declines such a name: a parameter's own type is not knowable here.
    else if (const auto * lit = right->as<ASTLiteral>(); lit && !bound_by_lambda)
    {
        /// A plain literal list is folded by the parser into one literal: `IN ('a')` is a String,
        /// `IN ('a','b')` a Tuple, `IN ['a']` an Array.
        if (lit->value.getType() == Field::Types::String)
        {
            /// Same form as a multi-element list, so every folded shape stores one spelling.
            right = castCollectionWithTimezone(Array{lit->value}, dt, timezone);
            wrapped = true;
        }
        else if (lit->value.getType() == Field::Types::Tuple)
        {
            if (auto rewritten = rewriteCollectionWithTimezone(lit->value.safeGet<Tuple>(), "tuple", dt, timezone))
            {
                right = std::move(rewritten);
                wrapped = true;
            }
        }
        else if (lit->value.getType() == Field::Types::Array)
        {
            if (auto rewritten = rewriteCollectionWithTimezone(lit->value.safeGet<Array>(), "array", dt, timezone))
            {
                right = std::move(rewritten);
                wrapped = true;
            }
        }
    }

    return wrapped;
}

const std::unordered_set<String> comparison_functions = {
    "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
};

class RewriteDateTimeLiteralsMatcher
{
public:
    struct Data
    {
        const ColumnsDescription & columns;
        const StorageID & table_id;
        const String & session_timezone;
        /// The parameters of the lambdas enclosing the node being visited. A lambda keeps its
        /// parameters as plain identifiers in the AST, and inside its body such a name shadows a
        /// storage column of the same name, so it must not be mistaken for that column.
        std::vector<String> lambda_parameters;
        bool modified = false;
    };

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & /*child*/)
    {
        /// A lambda body is walked by `visit` instead, which first records the names it binds.
        if (const auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
            return false;
        return !ast->as<ASTSelectQuery>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function = ast->as<ASTFunction>())
            visit(*function, data);
        else if (auto * assignment = ast->as<ASTAssignment>())
            visit(*assignment, data);
    }

    /// Wrap string literal in UPDATE SET when the target column is DateTime without explicit timezone
    static void visit(ASTAssignment & assignment, Data & data)
    {
        auto dt = getDateTimeColumnType(assignment.column_name, data.columns);
        if (!dt)
            return;

        auto & expr = assignment.children.at(0);
        if (const auto * lit = expr->as<ASTLiteral>(); lit && lit->value.getType() == Field::Types::String)
        {
            expr = wrapWithTimezone(expr, dt, data.session_timezone);
            data.modified = true;
        }
    }

    static void visit(ASTFunction & function, Data & data)
    {
        if (function.name == "lambda")
        {
            /// Walk the body with the parameter names bound, so an occurrence of one inside it is
            /// left alone while a real column mentioned there is still rewritten. The parameter
            /// list holds only identifiers, wrapped in a `tuple` or, for a single parameter, bare.
            if (!function.arguments || function.arguments->children.size() != 2)
                return;

            auto & arguments = function.arguments->children;
            const size_t enclosing_parameters = data.lambda_parameters.size();
            const auto & parameters = arguments[0];
            if (const auto * parameters_tuple = parameters->as<ASTFunction>(); parameters_tuple && parameters_tuple->arguments)
            {
                for (const auto & parameter : parameters_tuple->arguments->children)
                    if (const auto * parameter_identifier = parameter->as<ASTIdentifier>())
                        data.lambda_parameters.push_back(parameter_identifier->name());
            }
            else if (const auto * parameter_identifier = parameters->as<ASTIdentifier>())
            {
                data.lambda_parameters.push_back(parameter_identifier->name());
            }

            InDepthNodeVisitor<RewriteDateTimeLiteralsMatcher, true>(data).visit(arguments[1]);
            data.lambda_parameters.resize(enclosing_parameters);
        }
        else if (comparison_functions.contains(function.name))
        {
            if (tryWrapComparisonLiteral(
                    function, data.columns, data.table_id, data.session_timezone, data.lambda_parameters))
                data.modified = true;
        }
        else if (functionIsInOrGlobalInOperator(function.name))
        {
            if (tryWrapInLiterals(
                    function, data.columns, data.table_id, data.session_timezone, data.lambda_parameters))
                data.modified = true;
        }
    }
};

using RewriteDateTimeLiteralsVisitor = InDepthNodeVisitor<RewriteDateTimeLiteralsMatcher, true>;

}

ASTPtr rewriteDateTimeLiteralsWithTimezone(
    const ASTAlterCommand & alter_command,
    const ColumnsDescription & columns,
    const StorageID & table_id,
    const String & session_timezone)
{
    if (session_timezone.empty())
        return nullptr;

    auto query = alter_command.clone();
    auto & new_command = *query->as<ASTAlterCommand>();

    auto remove_child = [](auto & children, IAST *& erase_ptr)
    {
        auto it = std::find_if(children.begin(), children.end(), [&](const auto & ptr) { return ptr.get() == erase_ptr; });
        erase_ptr = nullptr;
        children.erase(it);
    };

    RewriteDateTimeLiteralsMatcher::Data data{columns, table_id, session_timezone, {}, false};
    RewriteDateTimeLiteralsVisitor visitor(data);

    if (new_command.update_assignments)
    {
        ASTPtr update_assignments = new_command.update_assignments->clone();
        remove_child(new_command.children, new_command.update_assignments);
        visitor.visit(update_assignments);
        new_command.update_assignments = new_command.children.emplace_back(std::move(update_assignments)).get();
    }
    if (new_command.predicate)
    {
        ASTPtr predicate = new_command.predicate->clone();
        remove_child(new_command.children, new_command.predicate);
        visitor.visit(predicate);
        new_command.predicate = new_command.children.emplace_back(std::move(predicate)).get();
    }

    if (!data.modified)
        return nullptr;

    return query;
}

}
