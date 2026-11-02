#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>

#include <span>

namespace DB
{

namespace
{

/// Returns the DateTime/DateTime64 type if `type` is a DateTime without an explicit timezone, also inside `Nullable` or
/// `LowCardinality`. Returns nullptr otherwise.
DataTypePtr getZonelessDateTimeType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(removeLowCardinality(type));

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

/// Returns the DateTime/DateTime64 column type if `identifier_name` refers to
/// a DateTime column without an explicit timezone. Returns nullptr otherwise.
DataTypePtr getDateTimeColumnType(const String & identifier_name, const ColumnsDescription & columns)
{
    const auto * desc = columns.tryGet(identifier_name);
    if (!desc)
        return nullptr;
    return getZonelessDateTimeType(desc->type);
}

/// The types of the parameters of the enclosing lambdas, by name. A parameter with a known type shadows the column with its
/// name. A type that is not known is nullptr, and then the identifier is matched with the columns by its name.
using LambdaParameters = std::unordered_map<String, DataTypePtr>;

/// The type of the element `element_names` of a tuple of type `type`, such as `n.event` of the parameter `n`, or nullptr.
DataTypePtr getTupleElementType(DataTypePtr type, std::span<const String> element_names)
{
    for (const auto & element_name : element_names)
    {
        const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple)
            return nullptr;
        const auto position = tuple->tryGetPositionByName(element_name);
        if (!position)
            return nullptr;
        type = tuple->getElement(*position);
    }
    return type;
}

/// The type of the lambda parameter that `identifier` names, or of its element, such as `n.event` in `n -> n.event = '...'`.
/// Returns std::nullopt if the identifier does not name a parameter with a known type.
std::optional<DataTypePtr> tryGetLambdaParameterType(const ASTIdentifier & identifier, const LambdaParameters & lambda_parameters)
{
    const auto it = lambda_parameters.find(identifier.name_parts.front());
    if (it == lambda_parameters.end() || !it->second)
        return std::nullopt;
    return getTupleElementType(it->second, std::span(identifier.name_parts).subspan(1));
}

/// The same for an identifier, which may name a parameter of an enclosing lambda or an element of one.
DataTypePtr getDateTimeColumnType(const ASTIdentifier & identifier, const ColumnsDescription & columns, const LambdaParameters & lambda_parameters)
{
    if (const auto parameter_type = tryGetLambdaParameterType(identifier, lambda_parameters))
        return *parameter_type ? getZonelessDateTimeType(*parameter_type) : nullptr;
    return getDateTimeColumnType(identifier.name(), columns);
}

/// The type of `argument` of a higher-order function if it is a column, a subcolumn or a lambda parameter with a known type,
/// or nullptr.
DataTypePtr getArgumentType(const ASTPtr & argument, const ColumnsDescription & columns, const LambdaParameters & lambda_parameters)
{
    const auto * identifier = argument->as<ASTIdentifier>();
    if (!identifier)
        return nullptr;
    if (const auto parameter_type = tryGetLambdaParameterType(*identifier, lambda_parameters))
        return *parameter_type;
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier->name());
    return column ? column->type : nullptr;
}

/// The types of the `num_parameters` parameters of the lambda that is the argument `lambda_position` of the higher-order
/// function `function`: the elements of its arrays, the key and the value of its map, and the accumulator of `arrayFold`.
/// A type that is not known is nullptr.
DataTypes getLambdaParameterTypes(
    const ASTFunction & function,
    size_t lambda_position,
    size_t num_parameters,
    const ColumnsDescription & columns,
    const LambdaParameters & lambda_parameters)
{
    DataTypes types(num_parameters);
    const auto & arguments = function.arguments->children;
    if (lambda_position != 0)
        return types;

    auto argument_type = [&](size_t position) -> DataTypePtr
    {
        return position < arguments.size() ? getArgumentType(arguments[position], columns, lambda_parameters) : nullptr;
    };
    auto element_type = [&](size_t position) -> DataTypePtr
    {
        const auto type = argument_type(position);
        const auto * array = typeid_cast<const DataTypeArray *>(type.get());
        return array ? array->getNestedType() : nullptr;
    };

    if (function.name == "arrayFold")
    {
        /// arrayFold((acc, x, ...) -> ..., array, ..., acc_initial)
        if (num_parameters > 0 && arguments.size() == num_parameters + 1)
        {
            types[0] = argument_type(arguments.size() - 1);
            for (size_t i = 1; i < num_parameters; ++i)
                types[i] = element_type(i);
        }
        return types;
    }

    /// mapFilter((key, value) -> ..., map) and the other functions of a map.
    if (num_parameters == 2 && arguments.size() == 2)
    {
        const auto type = argument_type(1);
        if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        {
            types[0] = map->getKeyType();
            types[1] = map->getValueType();
            return types;
        }
    }

    /// arrayMap((x, y) -> ..., array_x, array_y) and the other functions of arrays.
    for (size_t i = 0; i < num_parameters; ++i)
        types[i] = element_type(i + 1);
    return types;
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
bool tryWrapComparisonLiteral(ASTFunction & function, const ColumnsDescription & columns, const LambdaParameters & lambda_parameters, const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    bool wrapped = false;
    auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    /// Check left=identifier, right=string-literal
    if (const auto * id = left->as<ASTIdentifier>())
    {
        if (auto dt = getDateTimeColumnType(*id, columns, lambda_parameters))
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
        if (auto dt = getDateTimeColumnType(*id, columns, lambda_parameters))
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
bool tryWrapInLiterals(ASTFunction & function, const ColumnsDescription & columns, const LambdaParameters & lambda_parameters, const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    const auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    const auto * id = left->as<ASTIdentifier>();
    if (!id)
        return false;

    auto dt = getDateTimeColumnType(*id, columns, lambda_parameters);
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
        LambdaParameters lambda_parameters = {};
    };

    static bool needChildVisit(const ASTPtr & ast, const ASTPtr & /*child*/)
    {
        /// The body of a lambda is visited with the types of its parameters by the function it is an argument of.
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
        /// The body of a lambda is visited with the function it is an argument of, which tells the types of its parameters.
        if (function.arguments)
        {
            const auto & arguments = function.arguments->children;
            for (size_t i = 0; i < arguments.size(); ++i)
                if (auto * lambda = arguments[i]->as<ASTFunction>(); lambda && lambda->name == "lambda")
                    visitLambda(*lambda, function, i, data);
        }

        if (comparison_functions.contains(function.name))
        {
            if (tryWrapComparisonLiteral(function, data.columns, data.lambda_parameters, data.session_timezone))
                data.modified = true;
        }
        else if (in_functions.contains(function.name))
        {
            if (tryWrapInLiterals(function, data.columns, data.lambda_parameters, data.session_timezone))
                data.modified = true;
        }
    }

    /// Visit the body of `lambda(tuple(x, ...), body)`, the argument `lambda_position` of `function`, with the types of its
    /// parameters.
    static void visitLambda(ASTFunction & lambda, const ASTFunction & function, size_t lambda_position, Data & data)
    {
        if (!lambda.arguments || lambda.arguments->children.size() != 2)
            return;
        const auto * parameters = lambda.arguments->children[0]->as<ASTFunction>();
        if (!parameters || parameters->name != "tuple" || !parameters->arguments)
            return;

        const auto & parameter_asts = parameters->arguments->children;
        const auto types = getLambdaParameterTypes(function, lambda_position, parameter_asts.size(), data.columns, data.lambda_parameters);
        Data body_data{data.columns, data.session_timezone, false, data.lambda_parameters};
        for (size_t i = 0; i < parameter_asts.size(); ++i)
            if (const auto * identifier = parameter_asts[i]->as<ASTIdentifier>())
                body_data.lambda_parameters[identifier->name()] = types[i];

        InDepthNodeVisitor<RewriteDateTimeLiteralsMatcher, true>(body_data).visit(lambda.arguments->children[1]);
        data.modified |= body_data.modified;
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

bool rewriteDateTimeLiteralsWithTimezone(const ASTPtr & expression, const ColumnsDescription & columns, const String & session_timezone)
{
    if (session_timezone.empty() || !expression)
        return false;

    /// The visitor replaces only the arguments of the functions it visits, so the root of the expression stays the same.
    ASTPtr root = expression;
    RewriteDateTimeLiteralsMatcher::Data data{columns, session_timezone, false};
    RewriteDateTimeLiteralsVisitor(data).visit(root);

    /// The assignments of `TTL ... GROUP BY ... SET` are not children of the element.
    std::function<void(const ASTPtr &)> visit_group_by_assignments = [&](const ASTPtr & ast)
    {
        if (const auto * ttl_element = ast->as<ASTTTLElement>())
            for (auto assignment : ttl_element->group_by_assignments)
                RewriteDateTimeLiteralsVisitor(data).visit(assignment);
        for (const auto & child : ast->children)
            visit_group_by_assignments(child);
    };
    visit_group_by_assignments(root);

    return data.modified;
}

}
