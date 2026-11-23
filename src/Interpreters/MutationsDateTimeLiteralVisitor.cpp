#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Analyzer/SetUtils.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <fmt/ranges.h>

#include <optional>

namespace DB
{

namespace
{

DataTypePtr eligibleDateTimeType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(removeLowCardinality(type));

    if (const auto * dt = typeid_cast<const DataTypeDateTime *>(unwrapped.get()))
        return dt->hasExplicitTimeZone() ? nullptr : unwrapped;
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(unwrapped.get()))
        return dt64->hasExplicitTimeZone() ? nullptr : unwrapped;
    return nullptr;
}

bool isBoundBy(const ASTIdentifier & identifier, const std::vector<String> & names)
{
    const auto & parts = identifier.name_parts;
    return !parts.empty() && std::ranges::find(names, parts.front()) != names.end();
}

/// Expression aliases as the analyzer collects them: none inside a lambda or a subquery, nor a lambda's own.
void collectAliases(const IAST & ast, std::vector<String> & aliases)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda")
        return;
    /// `time AS time` still names the column.
    const auto * identifier = ast.as<ASTIdentifier>();
    if (auto alias = ast.tryGetAlias(); !alias.empty() && (!identifier || identifier->shortName() != alias))
        aliases.push_back(std::move(alias));
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>())
        return;
    for (const auto & child : ast.children)
        collectAliases(*child, aliases);
}

/// Returns the DateTime/DateTime64 column type if `column_name` refers to
/// a DateTime column without an explicit timezone. Returns nullptr otherwise.
DataTypePtr getDateTimeColumnType(const String & column_name, const ColumnsDescription & columns)
{
    const auto * desc = columns.tryGet(column_name);
    return desc ? eligibleDateTimeType(desc->type) : nullptr;
}

DataTypePtr getDateTimeColumnType(
    const ASTIdentifier & identifier,
    const ColumnsDescription & columns,
    const StorageID & table_id,
    bool bound_by_lambda = false)
{
    if (bound_by_lambda)
        return getDateTimeColumnType(identifier.name(), columns);

    const auto & parts = identifier.name_parts;

    /// Analyzer name-resolution order: whole name, then table-qualified, then database-and-table-qualified.
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

        /// A `Tuple(time DateTime)` element is not a real column, so only a subcolumn lookup sees `x.time`.
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
        if (auto dt = getDateTimeColumnType(*id, columns, table_id, isBoundBy(*id, lambda_parameters)))
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
        if (auto dt = getDateTimeColumnType(*id, columns, table_id, isBoundBy(*id, lambda_parameters)))
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

DataTypePtr withExplicitTimezone(const DataTypePtr & datetime_type, const String & timezone)
{
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(datetime_type.get()))
        return std::make_shared<DataTypeDateTime64>(dt64->getScale(), timezone);
    return std::make_shared<DataTypeDateTime>(timezone);
}

/// The value the `IN` set of the identical `SELECT` gives a string, or nullopt if the set drops it. `CAST` and
/// `toDateTime` read strings more leniently, and some differently: `'20000101'` is an epoch to the set, a date to them.
std::optional<Field> convertAsSetMember(const Field & value, const DataTypePtr & zoned_type)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto elements = getSetElementsForConstantValue(zoned_type, string_type->createColumnConst(1, value), string_type, {});
    const auto & column = *elements.front().column;
    if (column.empty())
        return std::nullopt;
    return column[0];
}

/// A `DateTime64` set value is stored as decimal seconds read through this type, which, unlike a date-time text, depends
/// on no parsing mode and keeps the sign of an instant in the second before 1970. A `DateTime` one is its epoch.
DataTypePtr decimalSecondsType(const DataTypePtr & datetime_type)
{
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(datetime_type.get()))
        return std::make_shared<DataTypeDecimal<Decimal128>>(DecimalUtils::max_precision<Decimal128>, dt64->getScale());
    return nullptr;
}

String toDecimalSeconds(const Field & datetime64)
{
    const auto & decimal = datetime64.safeGet<DecimalField<DateTime64>>();
    WriteBufferFromOwnString buf;
    writeText(decimal.getValue(), decimal.getScale(), buf);
    return buf.str();
}

ASTPtr castLiteral(Field value, const DataTypePtr & type)
{
    return makeASTFunction("CAST", make_intrusive<ASTLiteral>(std::move(value)), make_intrusive<ASTLiteral>(type->getName()));
}

/// Replaces each string with its set value. A `DateTime64` value needs a cast, so a collection that also holds other
/// elements is expanded to `expanded_function_name`.
template <typename Collection>
ASTPtr rewriteCollectionWithTimezone(
    const Collection & elements,
    std::string_view expanded_function_name,
    const DataTypePtr & datetime_type,
    const String & timezone)
{
    const auto zoned_type = withExplicitTimezone(datetime_type, timezone);
    const auto decimal_type = decimalSecondsType(datetime_type);

    /// Every string of `converted` is the decimal seconds of a `DateTime64` set value.
    Collection converted;
    size_t strings = 0;
    for (const auto & element : elements)
    {
        if (element.getType() != Field::Types::String)
        {
            converted.push_back(element);
            continue;
        }
        ++strings;
        if (auto value = convertAsSetMember(element, zoned_type))
            converted.push_back(decimal_type ? Field(toDecimalSeconds(*value)) : std::move(*value));
    }

    if (strings == 0)
        return nullptr;

    if (!decimal_type)
        return make_intrusive<ASTLiteral>(converted.empty() ? Field(Array{}) : Field(std::move(converted)));

    if (strings == elements.size())
        return castLiteral(Array(converted.begin(), converted.end()), std::make_shared<DataTypeArray>(decimal_type));

    auto function = makeASTFunction(expanded_function_name);
    auto & arguments = function->arguments->children;
    arguments.reserve(converted.size());
    for (auto & value : converted)
    {
        if (value.getType() == Field::Types::String)
            arguments.push_back(castLiteral(std::move(value), decimal_type));
        else
            arguments.push_back(make_intrusive<ASTLiteral>(std::move(value)));
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
    const std::vector<String> & lambda_parameters,
    const std::vector<String> & aliases)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    const auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    const auto * id = left->as<ASTIdentifier>();
    if (!id)
        return false;

    const bool bound_by_lambda = isBoundBy(*id, lambda_parameters);
    auto dt = getDateTimeColumnType(*id, columns, table_id, bound_by_lambda);
    if (!dt)
        return false;

    /// A set value would drop an alias, and fits only the column itself, not a lambda parameter or alias of its name.
    const bool wrap_strings = bound_by_lambda || (!id->compound() && isBoundBy(*id, aliases)) || !right->tryGetAlias().empty();

    bool wrapped = false;
    const auto zoned_type = withExplicitTimezone(dt, timezone);
    const auto decimal_type = decimalSecondsType(dt);

    /// The right side of IN can be an ASTFunction (tuple) or ASTExpressionList.
    /// Walk its children and rewrite string literals.
    auto wrap_children = [&](ASTs & children)
    {
        ASTs rewritten;
        rewritten.reserve(children.size());
        for (const auto & child : children)
        {
            const auto * lit = child->as<ASTLiteral>();
            if (!lit || lit->value.getType() != Field::Types::String)
            {
                rewritten.push_back(child);
                continue;
            }

            wrapped = true;
            if (wrap_strings || !lit->alias.empty())
            {
                rewritten.push_back(wrapWithTimezone(child, dt, timezone));
                continue;
            }

            auto value = convertAsSetMember(lit->value, zoned_type);
            if (!value)
                continue;

            if (decimal_type)
                rewritten.push_back(castLiteral(toDecimalSeconds(*value), decimal_type));
            else
                rewritten.push_back(make_intrusive<ASTLiteral>(std::move(*value)));
        }
        children = std::move(rewritten);
    };

    if (auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple" && tuple_func->arguments)
        wrap_children(tuple_func->arguments->children);
    else if (auto * expr_list = right->as<ASTExpressionList>())
        wrap_children(expr_list->children);
    else if (const auto * lit = right->as<ASTLiteral>(); lit && !wrap_strings)
    {
        /// A plain literal list is folded by the parser into one literal: `IN ('a')` is a String,
        /// `IN ('a','b')` a Tuple, `IN ['a']` an Array.
        ASTPtr rewritten;
        if (lit->value.getType() == Field::Types::String)
            rewritten = rewriteCollectionWithTimezone(Array{lit->value}, "array", dt, timezone);
        else if (lit->value.getType() == Field::Types::Tuple)
            rewritten = rewriteCollectionWithTimezone(lit->value.safeGet<Tuple>(), "tuple", dt, timezone);
        else if (lit->value.getType() == Field::Types::Array)
            rewritten = rewriteCollectionWithTimezone(lit->value.safeGet<Array>(), "array", dt, timezone);

        if (rewritten)
        {
            right = std::move(rewritten);
            wrapped = true;
        }
    }

    return wrapped;
}

[[clang::no_destroy]] const std::unordered_set<String> comparison_functions = {
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
        /// Parameters of the lambdas enclosing the visited node. A lambda keeps its parameters as plain
        /// identifiers in the AST, and inside its body such a name shadows a storage column of the same name.
        std::vector<String> lambda_parameters;
        std::vector<String> aliases;
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
            /// The parameter list holds only identifiers, wrapped in a `tuple` or, for a single one, bare.
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

            const size_t enclosing_aliases = data.aliases.size();
            collectAliases(*arguments[1], data.aliases);

            InDepthNodeVisitor<RewriteDateTimeLiteralsMatcher, true>(data).visit(arguments[1]);
            data.lambda_parameters.resize(enclosing_parameters);
            data.aliases.resize(enclosing_aliases);
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
                    function, data.columns, data.table_id, data.session_timezone, data.lambda_parameters, data.aliases))
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

    RewriteDateTimeLiteralsMatcher::Data data{columns, table_id, session_timezone, {}, {}, false};
    if (new_command.update_assignments)
        collectAliases(*new_command.update_assignments, data.aliases);
    if (new_command.predicate)
        collectAliases(*new_command.predicate, data.aliases);
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
