#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{

namespace
{

/// Returns the DateTime/DateTime64 column type if `identifier_name` refers to
/// a DateTime column without an explicit timezone. Returns nullptr otherwise.
DataTypePtr getDateTimeColumnType(const String & identifier_name, const ColumnsDescription & columns)
{
    const auto * desc = columns.tryGet(identifier_name);
    if (!desc)
        return nullptr;

    auto unwrapped = removeNullable(removeLowCardinality(desc->type));

    if (const auto * dt = typeid_cast<const DataTypeDateTime *>(unwrapped.get()))
    {
        if (!dt->hasExplicitTimeZone())
            return unwrapped;
    }
    else if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(unwrapped.get()))
    {
        if (!dt64->hasExplicitTimeZone())
            return unwrapped;
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
bool tryWrapComparisonLiteral(ASTFunction & function, const ColumnsDescription & columns, const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    bool wrapped = false;
    auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    /// Check left=identifier, right=string-literal
    if (const auto * id = left->as<ASTIdentifier>())
    {
        if (auto dt = getDateTimeColumnType(id->name(), columns))
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
        if (auto dt = getDateTimeColumnType(id->name(), columns))
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

/// For an IN expression like `column IN ('dt1', 'dt2')`, wrap each string literal.
/// Returns true if any literal was wrapped.
bool tryWrapInLiterals(ASTFunction & function, const ColumnsDescription & columns, const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    const auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    const auto * id = left->as<ASTIdentifier>();
    if (!id)
        return false;

    auto dt = getDateTimeColumnType(id->name(), columns);
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

    return wrapped;
}

const std::unordered_set<String> comparison_functions = {
    "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
};

const std::unordered_set<String> in_functions = {
    "in", "notIn", "globalIn", "globalNotIn",
};

class RewriteDateTimeLiteralsMatcher
{
public:
    struct Data
    {
        const ColumnsDescription & columns;
        const String & session_timezone;
        bool modified = false;
    };

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & /*child*/)
    {
        return !ast->as<ASTSelectQuery>();
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function = ast->as<ASTFunction>())
            visit(*function, data);
    }

    static void visit(ASTFunction & function, Data & data)
    {
        if (comparison_functions.contains(function.name))
        {
            if (tryWrapComparisonLiteral(function, data.columns, data.session_timezone))
                data.modified = true;
        }
        else if (in_functions.contains(function.name))
        {
            if (tryWrapInLiterals(function, data.columns, data.session_timezone))
                data.modified = true;
        }
    }
};

using RewriteDateTimeLiteralsVisitor = InDepthNodeVisitor<RewriteDateTimeLiteralsMatcher, true>;

}

ASTPtr rewriteDateTimeLiteralsWithTimezone(
    const ASTAlterCommand & alter_command,
    const ColumnsDescription & columns,
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

    RewriteDateTimeLiteralsMatcher::Data data{columns, session_timezone, false};
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
