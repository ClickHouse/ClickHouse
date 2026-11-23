#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <functional>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
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

/// Functions that parse their string argument in the session timezone, with the number of arguments
/// they have when the optional timezone, which would come right after them, is not given.
const std::unordered_map<String, size_t> timezone_less_parsers = {
    {"toDateTime", 1},
    {"toDateTime64", 2},
    {"parseDateTimeBestEffort", 1},
    {"parseDateTime", 2},
};

/// Returns `type` with `timezone` set if it is a `DateTime` or `DateTime64` without an explicit timezone,
/// possibly wrapped in `Nullable` or `LowCardinality`. Returns nullptr otherwise.
DataTypePtr addTimezoneToType(const DataTypePtr & type, const String & timezone)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        auto nested = addTimezoneToType(nullable->getNestedType(), timezone);
        return nested ? std::make_shared<DataTypeNullable>(nested) : nullptr;
    }
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        auto dictionary = addTimezoneToType(low_cardinality->getDictionaryType(), timezone);
        return dictionary ? std::make_shared<DataTypeLowCardinality>(dictionary) : nullptr;
    }
    if (const auto * dt = typeid_cast<const DataTypeDateTime *>(type.get()); dt && !dt->hasExplicitTimeZone())
        return std::make_shared<DataTypeDateTime>(timezone);
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(type.get()); dt64 && !dt64->hasExplicitTimeZone())
        return std::make_shared<DataTypeDateTime64>(dt64->getScale(), timezone);
    return nullptr;
}

/// A string literal inside a function call is parsed by the background mutation thread in the server timezone.
/// Makes the session timezone explicit, `toDateTime('..')` becomes `toDateTime('..', 'tz')`, and
/// `CAST('..' AS DateTime)` becomes `CAST('..' AS DateTime('tz'))`. The node is changed in place, so its alias is kept.
/// Returns true if it was rewritten.
bool tryAddTimezoneToLiteralParser(ASTPtr & ast, const String & timezone)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.empty())
        return false;

    auto & arguments = function->arguments->children;
    const auto * literal = arguments[0]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return false;

    if (function->name == "CAST" && arguments.size() == 2)
    {
        const auto * type_name = arguments[1]->as<ASTLiteral>();
        auto type = type_name && type_name->value.getType() == Field::Types::String
            ? DataTypeFactory::instance().tryGet(type_name->value.safeGet<String>())
            : nullptr;
        if (!type)
            return false;

        auto type_with_timezone = addTimezoneToType(type, timezone);
        if (!type_with_timezone)
            return false;

        arguments[1] = make_intrusive<ASTLiteral>(Field(type_with_timezone->getName()));
        return true;
    }

    auto parser = timezone_less_parsers.find(function->name);
    if (parser == timezone_less_parsers.end())
        return false;

    /// The overloads without a timezone that have another number of arguments: `parseDateTime` with its default
    /// format, and `toDateTime` with a scale (`toDateTime('..', 3)` is a `DateTime64`).
    const bool is_parse_with_default_format = function->name == "parseDateTime" && arguments.size() == 1;
    const auto * second_literal = arguments.size() == 2 ? arguments[1]->as<ASTLiteral>() : nullptr;
    const bool is_to_datetime_with_scale
        = function->name == "toDateTime" && second_literal && second_literal->value.getType() != Field::Types::String;

    if (arguments.size() != parser->second && !is_parse_with_default_format && !is_to_datetime_with_scale)
        return false;

    if (is_parse_with_default_format)
        arguments.push_back(make_intrusive<ASTLiteral>(Field(String("%Y-%m-%d %H:%i:%s"))));

    arguments.push_back(make_intrusive<ASTLiteral>(Field(timezone)));
    return true;
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

/// Functions whose result depends on the exact type of an argument.
const std::unordered_set<String> type_inspecting_functions = {
    "toTypeName", "toColumnTypeName", "dumpColumnStructure", "defaultValueOfArgumentType",
    "dynamicType", "dynamicElement", "variantType", "variantElement", "getTypeSerializationStreams",
};

/// A `Dynamic`, `Variant` or `JSON` column keeps the exact type of what is stored in it, so the timezone
/// of the type would be visible there. Such assignments are left as they were submitted.
bool keepsExactType(const String & column_name, const ColumnsDescription & columns)
{
    const auto * desc = columns.tryGet(column_name);
    return desc && (desc->type->hasDynamicStructure() || desc->type->getName().contains("Variant"));
}

/// An alias can be used where the function that returns the type of its argument is, and the rewritten call may be the alias.
bool inspectsTypeOfAlias(const ASTPtr & ast, const ColumnsDescription & columns)
{
    if (const auto * function = ast->as<ASTFunction>(); function && type_inspecting_functions.contains(function->name) && function->arguments)
    {
        std::function<bool(const ASTPtr &)> has_alias = [&](const ASTPtr & node)
        {
            if (const auto * identifier = node->as<ASTIdentifier>(); identifier && !columns.has(identifier->name()))
                return true;
            return std::any_of(node->children.begin(), node->children.end(), has_alias);
        };
        if (has_alias(function->arguments))
            return true;
    }
    return std::any_of(ast->children.begin(), ast->children.end(), [&](const ASTPtr & child) { return inspectsTypeOfAlias(child, columns); });
}

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
        if (ast->as<ASTSelectQuery>())
            return false;

        /// The timezone is made explicit by changing the type of the rewritten call, from `DateTime` to
        /// `DateTime('tz')`. The value is the same, but a function that returns the name of its argument's type
        /// would report the changed one. Leave such expressions as they were submitted: they do not depend on the value.
        if (const auto * function = ast->as<ASTFunction>())
            return !type_inspecting_functions.contains(function->name);

        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * function = ast->as<ASTFunction>())
        {
            visit(*function, data);
            if (tryAddTimezoneToLiteralParser(ast, data.session_timezone))
                data.modified = true;
        }
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
        for (auto & child : update_assignments->children)
        {
            const auto * assignment = child->as<ASTAssignment>();
            if ((assignment && keepsExactType(assignment->column_name, columns)) || inspectsTypeOfAlias(child, columns))
                continue;
            visitor.visit(child);
        }
        new_command.update_assignments = new_command.children.emplace_back(std::move(update_assignments)).get();
    }
    if (new_command.predicate && !inspectsTypeOfAlias(new_command.predicate, columns))
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
