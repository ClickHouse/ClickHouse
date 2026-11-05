#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Analyzer/ListNode.h>
#include <Core/Settings.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/FieldToDataType.h>
#include <Functions/FunctionFactory.h>

#include <span>

namespace DB
{

namespace Setting
{
    extern const SettingsBool prefer_column_name_to_alias;
}

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

/// The types of the aliases of the expression, by name, which shadow the columns with their names, such as `c` in
/// `has([1, 2] AS c, 1) AND arrayExists(x -> x = 1, c)`. A type that is not known is nullptr, and then the identifier with
/// the name of the alias has no known type either.
using AliasTypes = std::unordered_map<String, DataTypePtr>;

/// The type of the alias that `identifier` names, or of its element, which is nullptr if it is not known. Returns std::nullopt
/// if the identifier does not name an alias.
///
/// The mutation runs with the settings of the server or of the session, depending on the engine, which may differ from those of
/// this query: with `prefer_column_name_to_alias` there, the identifier names the column with its name instead, if there is
/// one. The callers check that what they derive from the type holds for the column too.
std::optional<DataTypePtr> tryGetAliasType(const ASTIdentifier & identifier, const AliasTypes & aliases)
{
    const auto it = aliases.find(identifier.name_parts.front());
    if (it == aliases.end())
        return std::nullopt;
    if (!it->second)
        return DataTypePtr{};
    return getTupleElementType(it->second, std::span(identifier.name_parts).subspan(1));
}

/// The same for an identifier, which may name a parameter of an enclosing lambda or an element of one, an alias, a column or a
/// subcolumn.
DataTypePtr getDateTimeColumnType(
    const ASTIdentifier & identifier,
    const ColumnsDescription & columns,
    const AliasTypes & aliases,
    const LambdaParameters & lambda_parameters)
{
    if (const auto parameter_type = tryGetLambdaParameterType(identifier, lambda_parameters))
        return *parameter_type ? getZonelessDateTimeType(*parameter_type) : nullptr;
    if (const auto alias_type = tryGetAliasType(identifier, aliases))
    {
        if (!*alias_type)
            return nullptr;
        auto datetime_type = getZonelessDateTimeType(*alias_type);
        /// If the identifier can name a column instead, the literal converted in the time zone of the session is right for both
        /// only if the column is a `DateTime` or `DateTime64` without a time zone too, such as `event` in `toNullable(event) AS
        /// event` or `toDateTime64(event, 3) AS event`: it is compared with either of them as the session would compare the string.
        const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier.name());
        if (datetime_type && column && !getZonelessDateTimeType(column->type))
            return nullptr;
        return datetime_type;
    }
    /// A subcolumn too, such as `n.ts` of `n Tuple(ts DateTime)`.
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier.name());
    return column ? getZonelessDateTimeType(column->type) : nullptr;
}

/// The type of `argument` of a higher-order function if it is a column, a subcolumn, a lambda parameter or an alias with a
/// known type, or nullptr.
DataTypePtr getArgumentType(
    const ASTPtr & argument, const ColumnsDescription & columns, const AliasTypes & aliases, const LambdaParameters & lambda_parameters)
{
    const auto * identifier = argument->as<ASTIdentifier>();
    if (!identifier)
        return nullptr;
    if (const auto parameter_type = tryGetLambdaParameterType(*identifier, lambda_parameters))
        return *parameter_type;
    if (const auto alias_type = tryGetAliasType(*identifier, aliases))
    {
        /// The lambda parameters get their types from this type, so it must be the type of the column too if the identifier can
        /// name one.
        const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier->name());
        if (*alias_type && column && !column->type->equals(**alias_type))
            return nullptr;
        return *alias_type;
    }
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier->name());
    return column ? column->type : nullptr;
}

/// The type of a non-lambda argument of a higher-order function: a column, a subcolumn, a lambda parameter or an alias with a
/// known type, or a literal. Returns nullptr if it is not known.
DataTypePtr getArgumentOrLiteralType(
    const ASTPtr & argument, const ColumnsDescription & columns, const AliasTypes & aliases, const LambdaParameters & lambda_parameters)
{
    if (const auto * literal = argument->as<ASTLiteral>())
        return applyVisitor(FieldToDataType(), literal->value);
    return getArgumentType(argument, columns, aliases, lambda_parameters);
}

/// The number of the parameters of `lambda(tuple(x, ...), body)`, or std::nullopt if `ast` is not such a lambda.
std::optional<size_t> tryGetLambdaParameterCount(const ASTPtr & ast)
{
    const auto * lambda = ast->as<ASTFunction>();
    if (!lambda || lambda->name != "lambda" || !lambda->arguments || lambda->arguments->children.size() != 2)
        return std::nullopt;
    const auto * parameters = lambda->arguments->children[0]->as<ASTFunction>();
    if (!parameters || parameters->name != "tuple" || !parameters->arguments)
        return std::nullopt;
    return parameters->arguments->children.size();
}

/// The types of the `num_parameters` parameters of the lambda that is the argument `lambda_position` of the higher-order
/// function `function`, as the function itself binds them (`getLambdaArgumentTypes`, as the analyzer calls it): the elements
/// of its arrays, also unpacked from an array of tuples, after its fixed arguments, the key and the value of its map, or the
/// accumulator of `arrayFold`. The function needs the types of all its other arguments, so if one of them is not known,
/// such as the type of an expression, none of the types are known (nullptr), and the parameters are matched with the columns
/// by their names.
DataTypes getLambdaParameterTypes(
    const ASTFunction & function,
    size_t lambda_position,
    size_t num_parameters,
    const ColumnsDescription & columns,
    const AliasTypes & aliases,
    const LambdaParameters & lambda_parameters,
    const ContextPtr & context)
{
    DataTypes unknown(num_parameters);
    const auto resolver = FunctionFactory::instance().tryGet(function.name, context);
    if (!resolver || !resolver->isHigherOrderFunction())
        return unknown;

    const auto & arguments = function.arguments->children;
    DataTypes argument_types(arguments.size());
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (const auto lambda_parameter_count = tryGetLambdaParameterCount(arguments[i]))
            argument_types[i] = std::make_shared<DataTypeFunction>(DataTypes(*lambda_parameter_count, nullptr), nullptr);
        else
            argument_types[i] = getArgumentOrLiteralType(arguments[i], columns, aliases, lambda_parameters);
        if (!argument_types[i])
            return unknown;
    }

    resolver->getLambdaArgumentTypes(argument_types);

    const auto * function_type = typeid_cast<const DataTypeFunction *>(argument_types[lambda_position].get());
    if (!function_type || function_type->getArgumentTypes().size() != num_parameters)
        return unknown;
    return function_type->getArgumentTypes();
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
    ASTFunction & function, const ColumnsDescription & columns, const AliasTypes & aliases, const LambdaParameters & lambda_parameters,
    const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    bool wrapped = false;
    auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    /// Check left=identifier, right=string-literal
    if (const auto * id = left->as<ASTIdentifier>())
    {
        if (auto dt = getDateTimeColumnType(*id, columns, aliases, lambda_parameters))
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
        if (auto dt = getDateTimeColumnType(*id, columns, aliases, lambda_parameters))
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
bool tryWrapInLiterals(
    ASTFunction & function, const ColumnsDescription & columns, const AliasTypes & aliases, const LambdaParameters & lambda_parameters,
    const String & timezone)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return false;

    const auto & left = function.arguments->children[0];
    auto & right = function.arguments->children[1];

    const auto * id = left->as<ASTIdentifier>();
    if (!id)
        return false;

    auto dt = getDateTimeColumnType(*id, columns, aliases, lambda_parameters);
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

/// Adds the aliases defined in `ast` to `aliases`, also in the assignments of `TTL ... GROUP BY ... SET`, which are not its
/// children, as the analyzer scopes them: not those inside a lambda or a subquery, which are visible only there, nor the alias
/// of a lambda, which names a function rather than an expression, but the alias of a subquery, as in `(SELECT ...) AS s`. An
/// identifier aliased with its own name, as in `event AS event`, still names the column.
void collectAliases(const ASTPtr & ast, NameSet & aliases)
{
    if (!ast)
        return;
    const auto * function = ast->as<ASTFunction>();
    if ((function && function->name == "lambda") || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>())
        return;
    if (const String alias = ast->tryGetAlias(); !alias.empty())
        if (const auto * identifier = ast->as<ASTIdentifier>(); !identifier || identifier->name() != alias)
            aliases.insert(alias);
    /// The alias of a subquery is in this scope, the aliases inside it are not.
    if (ast->as<ASTSubquery>())
        return;
    if (const auto * ttl_element = ast->as<ASTTTLElement>())
        for (const auto & assignment : ttl_element->group_by_assignments)
            collectAliases(assignment, aliases);
    for (const auto & child : ast->children)
        collectAliases(child, aliases);
}

/// The aliases `names` with unknown types.
AliasTypes getUnknownAliasTypes(const NameSet & names)
{
    AliasTypes alias_types;
    for (const auto & name : names)
        alias_types.emplace(name, nullptr);
    return alias_types;
}

/// The types of the aliases `aliases` defined in `expressions`, which the analyzer gives them against the table with the
/// settings of the query, as `MutationsInterpreter` resolves the expressions to validate the mutation: the type of the aliased
/// expression, in which an identifier with the name of the alias names the column, as in `identity(event) AS event`.
AliasTypes getAliasTypes(
    const ASTs & expressions,
    const NameSet & aliases,
    const StoragePtr & table,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    AliasTypes alias_types = getUnknownAliasTypes(aliases);
    /// With `prefer_column_name_to_alias`, the analyzer gives an identifier the type of the column with its name rather than
    /// of the alias, and the mutation may run with other settings, so the types of the aliases are not known.
    if (context->getSettingsRef()[Setting::prefer_column_name_to_alias])
        return alias_types;

    const Names names(aliases.begin(), aliases.end());
    if (names.empty())
        return alias_types;

    /// The identifiers go first, because an expression of the list, such as a matcher, may be expanded to several nodes.
    auto list = make_intrusive<ASTExpressionList>();
    for (const auto & name : names)
        list->children.push_back(make_intrusive<ASTIdentifier>(name));
    for (const auto & expression : expressions)
        list->children.push_back(expression->clone());

    auto execution_context = Context::createCopy(context);
    auto query_tree = buildQueryTree(list, execution_context);
    /// The expressions are only analyzed and nothing is read, so the table is not locked.
    auto table_node = std::make_shared<TableNode>(table, TableLockHolder{}, storage_snapshot);
    QueryAnalyzer(/*only_analyze=*/true).resolve(query_tree, table_node, execution_context);

    const auto & nodes = query_tree->as<const ListNode &>().getNodes();
    for (size_t i = 0; i < names.size(); ++i)
        alias_types[names[i]] = nodes[i]->getResultType();
    return alias_types;
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
        /// The aliases of the expression, which shadow the columns with their names.
        const AliasTypes & aliases;
        const String & session_timezone;
        /// To find the higher-order functions that bind the parameters of a lambda.
        ContextPtr context;
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
            if (tryWrapComparisonLiteral(function, data.columns, data.aliases, data.lambda_parameters, data.session_timezone))
                data.modified = true;
        }
        else if (in_functions.contains(function.name))
        {
            if (tryWrapInLiterals(function, data.columns, data.aliases, data.lambda_parameters, data.session_timezone))
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
        const auto types = getLambdaParameterTypes(
            function, lambda_position, parameter_asts.size(), data.columns, data.aliases, data.lambda_parameters, data.context);
        /// An alias in the body shadows the columns and the outer aliases with its name in the body only, and its type is not
        /// known.
        NameSet body_alias_names;
        collectAliases(lambda.arguments->children[1], body_alias_names);
        AliasTypes body_aliases = data.aliases;
        for (const auto & name : body_alias_names)
            body_aliases[name] = nullptr;

        Data body_data{data.columns, body_aliases, data.session_timezone, data.context, false, data.lambda_parameters};
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
    const StoragePtr & table,
    const StorageSnapshotPtr & storage_snapshot,
    const String & session_timezone,
    const ContextPtr & context)
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

    /// The predicate and the values of the assignments, which `MutationsInterpreter` resolves together.
    ASTs expressions;
    if (new_command.predicate)
        expressions.push_back(new_command.predicate->ptr());
    if (new_command.update_assignments)
        for (const auto & child : new_command.update_assignments->children)
            if (const auto * assignment = child->as<ASTAssignment>())
                expressions.push_back(assignment->expression());

    NameSet alias_names;
    for (const auto & expression : expressions)
        collectAliases(expression, alias_names);
    const AliasTypes aliases = getAliasTypes(expressions, alias_names, table, storage_snapshot, context);

    const ColumnsDescription & columns = storage_snapshot->metadata->getColumns();
    RewriteDateTimeLiteralsMatcher::Data data{columns, aliases, session_timezone, context, false};
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

bool rewriteDateTimeLiteralsWithTimezone(
    const ASTPtr & expression, const ColumnsDescription & columns, const String & session_timezone, const ContextPtr & context)
{
    if (session_timezone.empty() || !expression)
        return false;

    /// The visitor replaces only the arguments of the functions it visits, so the root of the expression stays the same.
    ASTPtr root = expression;
    /// A `TTL` does not allow aliases, so their types are not needed.
    NameSet alias_names;
    collectAliases(root, alias_names);
    const AliasTypes aliases = getUnknownAliasTypes(alias_names);
    RewriteDateTimeLiteralsMatcher::Data data{columns, aliases, session_timezone, context, false};
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
