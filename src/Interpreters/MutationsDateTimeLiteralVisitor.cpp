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

/// A parameter of an enclosing lambda. A parameter with a known type shadows the column with its name. A type that is not known
/// is nullptr, and then the identifier is matched with the columns by its name, unless the parameter is one of a lambda whose
/// body is shared by several invocations, which may bind it to other types (see `visitLambda`).
struct LambdaParameter
{
    DataTypePtr type;
    bool shadows_columns = false;
};

/// The parameters of the enclosing lambdas, by name.
using LambdaParameters = std::unordered_map<String, LambdaParameter>;

/// The type of the element `element_names` of a tuple of type `type`, such as `n.event` of the parameter `n`, or nullptr. An
/// element of a `Nullable` tuple is `Nullable` too, if it can be, as the analyzer types it.
DataTypePtr getTupleElementType(DataTypePtr type, std::span<const String> element_names)
{
    for (const auto & element_name : element_names)
    {
        const bool is_nullable = type->isNullable();
        const auto * tuple = typeid_cast<const DataTypeTuple *>(removeNullable(type).get());
        if (!tuple)
            return nullptr;
        const auto position = tuple->tryGetPositionByName(element_name);
        if (!position)
            return nullptr;
        type = tuple->getElement(*position);
        if (is_nullable)
            type = makeNullableSafe(type);
    }
    return type;
}

/// The precision of a `DateTime` or `DateTime64` type: 0 for a `DateTime`.
UInt32 getDateTimeScale(const IDataType & type)
{
    if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(&type))
        return dt64->getScale();
    return 0;
}

/// The type of the lambda parameter that `identifier` names, or of its element, such as `n.event` in `n -> n.event = '...'`.
/// The type is nullptr if the parameter shadows the column with its name but its type is not known. Returns std::nullopt if the
/// identifier does not name a parameter that shadows the column.
std::optional<DataTypePtr> tryGetLambdaParameterType(const ASTIdentifier & identifier, const LambdaParameters & lambda_parameters)
{
    const auto it = lambda_parameters.find(identifier.name_parts.front());
    if (it == lambda_parameters.end())
        return std::nullopt;
    if (!it->second.type)
        return it->second.shadows_columns ? std::optional<DataTypePtr>(DataTypePtr{}) : std::nullopt;
    return getTupleElementType(it->second.type, std::span(identifier.name_parts).subspan(1));
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

/// The type with which a string literal compared with an identifier is converted.
struct LiteralConversion
{
    DataTypePtr type;
    /// If the identifier names an alias, or a column of another precision instead with other settings, the type of the column
    /// without a time zone and the name of its whole type, which tells where the mutation runs which of the two the identifier
    /// names, see `getDateTimeColumnType`.
    DataTypePtr column_type = nullptr;
    String column_type_name = {};

    explicit operator bool() const { return type != nullptr; }
};

/// The same for an identifier, which may name a parameter of an enclosing lambda or an element of one, an alias, a column or a
/// subcolumn.
LiteralConversion getDateTimeColumnType(
    const ASTIdentifier & identifier,
    const ColumnsDescription & columns,
    const AliasTypes & aliases,
    const LambdaParameters & lambda_parameters)
{
    if (const auto parameter_type = tryGetLambdaParameterType(identifier, lambda_parameters))
        return {*parameter_type ? getZonelessDateTimeType(*parameter_type) : nullptr};
    if (const auto alias_type = tryGetAliasType(identifier, aliases))
    {
        if (!*alias_type)
            return {};
        auto datetime_type = getZonelessDateTimeType(*alias_type);
        /// If the identifier can name a column or a subcolumn instead, the literal converted in the time zone of the session is
        /// right for both only if the column is a `DateTime` or `DateTime64` without a time zone too, such as `event` in
        /// `toNullable(event) AS event`. If their precisions differ, such as in `toDateTime64(event, 3) AS event` of a `DateTime`
        /// column, the comparison reads the string with the precision of the one that the identifier names, which depends on the
        /// settings with which the mutation runs, so the literal is converted with the precision of the identifier there.
        const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier.name());
        if (datetime_type && column)
        {
            auto column_datetime_type = getZonelessDateTimeType(column->type);
            if (!column_datetime_type)
                return {};
            const auto alias_scale = getDateTimeScale(*datetime_type);
            const auto column_scale = getDateTimeScale(*column_datetime_type);
            if (alias_scale != column_scale)
                return {datetime_type, column_datetime_type, column->type->getName()};
        }
        return {datetime_type};
    }
    /// A subcolumn too, such as `n.ts` of `n Tuple(ts DateTime)`.
    const auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier.name());
    return {column ? getZonelessDateTimeType(column->type) : nullptr};
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

/// Wraps `literal_ast` if it is a string literal that `conversion` converts, for a comparison with `identifier_ast`. Returns
/// whether it did. If the identifier names an alias or a column of another precision depending on the settings, the literal is
/// read in the time zone with the precision of each, and the one of the type that the identifier has where the mutation runs is
/// taken: `if(toTypeName(event) = 'DateTime', toDateTime('...', 'tz'), toDateTime64('...', 3, 'tz'))`. So it is read as the
/// comparison of the string reads it, with the same range and rounding, also before 1970.
bool tryWrapLiteral(ASTPtr & literal_ast, const ASTPtr & identifier_ast, const LiteralConversion & conversion, const String & timezone)
{
    const auto * literal = literal_ast->as<ASTLiteral>();
    if (!conversion || !literal || literal->value.getType() != Field::Types::String)
        return false;
    if (!conversion.column_type)
    {
        literal_ast = wrapWithTimezone(literal_ast, conversion.type, timezone);
        return true;
    }
    auto names_column = makeASTFunction(
        "equals", makeASTFunction("toTypeName", identifier_ast->clone()), make_intrusive<ASTLiteral>(Field(conversion.column_type_name)));
    auto read_for_column = wrapWithTimezone(literal_ast->clone(), conversion.column_type, timezone);
    auto read_for_alias = wrapWithTimezone(literal_ast, conversion.type, timezone);
    literal_ast = makeASTFunction("if", std::move(names_column), std::move(read_for_column), std::move(read_for_alias));
    return true;
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
        if (tryWrapLiteral(right, left, getDateTimeColumnType(*id, columns, aliases, lambda_parameters), timezone))
            wrapped = true;
    }

    /// Check right=identifier, left=string-literal (e.g. '2000-01-01' <= time)
    if (const auto * id = right->as<ASTIdentifier>())
    {
        if (tryWrapLiteral(left, right, getDateTimeColumnType(*id, columns, aliases, lambda_parameters), timezone))
            wrapped = true;
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

    const auto conversion = getDateTimeColumnType(*id, columns, aliases, lambda_parameters);
    if (!conversion)
        return false;

    bool wrapped = false;

    /// The right side of IN can be an ASTFunction (tuple) or ASTExpressionList.
    /// Walk its children and wrap string literals.
    auto wrap_children = [&](ASTs & children)
    {
        for (auto & child : children)
            if (tryWrapLiteral(child, left, conversion, timezone))
                wrapped = true;
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
/// identifier aliased with its own name, as in `event AS event`, still names the column. If `aliased_expressions` is not
/// nullptr, the aliased expressions are added to it by the names of their aliases too.
void collectAliases(const ASTPtr & ast, NameSet & aliases, std::unordered_map<String, ASTPtr> * aliased_expressions = nullptr)
{
    if (!ast)
        return;
    const auto * function = ast->as<ASTFunction>();
    if ((function && function->name == "lambda") || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>())
        return;
    if (const String alias = ast->tryGetAlias(); !alias.empty())
    {
        if (const auto * identifier = ast->as<ASTIdentifier>(); !identifier || identifier->name() != alias)
        {
            aliases.insert(alias);
            if (aliased_expressions)
                aliased_expressions->emplace(alias, ast);
        }
    }
    /// The alias of a subquery is in this scope, the aliases inside it are not.
    if (ast->as<ASTSubquery>())
        return;
    if (const auto * ttl_element = ast->as<ASTTTLElement>())
        for (const auto & assignment : ttl_element->group_by_assignments)
            collectAliases(assignment, aliases, aliased_expressions);
    for (const auto & child : ast->children)
        collectAliases(child, aliases, aliased_expressions);
}

/// Whether `ast` has an identifier whose name, or the first part of it, is one of `names`.
bool hasIdentifierWithName(const ASTPtr & ast, const NameSet & names)
{
    if (const auto * identifier = ast->as<ASTIdentifier>(); identifier && names.contains(identifier->name_parts.front()))
        return true;
    return std::ranges::any_of(ast->children, [&](const ASTPtr & child) { return hasIdentifierWithName(child, names); });
}

/// Whether every identifier of `ast` names a column or a subcolumn of the table, so that `ast` can be analyzed alone against it.
bool usesOnlyColumns(const ASTPtr & ast, const ColumnsDescription & columns)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        return columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier->name()).has_value();
    return std::ranges::all_of(ast->children, [&](const ASTPtr & child) { return usesOnlyColumns(child, columns); });
}

/// Adds the names of the identifiers and of the functions in `ast` to `names`, also in the assignments of
/// `TTL ... GROUP BY ... SET` and in lambdas and subqueries, to find the aliases of lambdas that the expression invokes. A name
/// that is not such an invocation, such as that of a column, only makes the lambda with that alias count as invoked.
void collectReferencedNames(const ASTPtr & ast, NameSet & names)
{
    if (!ast)
        return;
    if (const auto * identifier = ast->as<ASTIdentifier>())
        names.insert(identifier->name_parts.front());
    else if (const auto * function = ast->as<ASTFunction>())
        names.insert(function->name);
    if (const auto * ttl_element = ast->as<ASTTTLElement>())
        for (const auto & assignment : ttl_element->group_by_assignments)
            collectReferencedNames(assignment, names);
    for (const auto & child : ast->children)
        collectReferencedNames(child, names);
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
///
/// With `prefer_column_name_to_alias`, an identifier with the name of an alias names the column with that name, if there is
/// one, rather than the alias, while the mutation may run with other settings (a background mutation of `MergeTree` runs with
/// those of the server), where it names the alias. The type of such an alias is then analyzed from its expression rather than
/// from an identifier. The expression has that type with either setting if it does not refer to another alias with the name of
/// a column, which the two settings bind differently (an identifier with the name of its own alias names the column with both,
/// as in `identity(event) AS event`); otherwise the types of the aliases are not known. The callers check that what they derive
/// from the type of such an alias holds for the column too (see `getDateTimeColumnType` and `getArgumentType`).
AliasTypes getAliasTypes(
    const ASTs & expressions,
    const NameSet & aliases,
    const StoragePtr & table,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context)
{
    AliasTypes alias_types = getUnknownAliasTypes(aliases);
    const Names names(aliases.begin(), aliases.end());
    if (names.empty())
        return alias_types;

    /// The aliases with the names of columns, with `prefer_column_name_to_alias`, and the expressions of the aliases.
    NameSet aliases_of_columns;
    std::unordered_map<String, ASTPtr> aliased_expressions;
    if (context->getSettingsRef()[Setting::prefer_column_name_to_alias])
    {
        const auto & columns = storage_snapshot->metadata->getColumns();
        for (const auto & name : names)
            if (columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name))
                aliases_of_columns.insert(name);

        NameSet collected_aliases;
        for (const auto & expression : expressions)
            collectAliases(expression, collected_aliases, &aliased_expressions);
        for (const auto & [name, aliased_expression] : aliased_expressions)
        {
            NameSet other_aliases_of_columns = aliases_of_columns;
            other_aliases_of_columns.erase(name);
            if (hasIdentifierWithName(aliased_expression, other_aliases_of_columns))
                return alias_types;
        }
    }

    /// The identifiers, or the expressions of the aliases with the names of columns, go first, because an expression of the
    /// list, such as a matcher, may be expanded to several nodes. The copy of an expression does not define its alias again,
    /// while the aliases inside it, which it repeats with the same expressions, stay, since its lambdas and subqueries
    /// refer to them in their own scopes.
    auto list = make_intrusive<ASTExpressionList>();
    for (const auto & name : names)
    {
        if (aliases_of_columns.contains(name))
        {
            auto aliased_expression = aliased_expressions.at(name)->clone();
            aliased_expression->setAlias({});
            list->children.push_back(std::move(aliased_expression));
        }
        else
        {
            list->children.push_back(make_intrusive<ASTIdentifier>(name));
        }
    }
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
        /// The names of the identifiers and the functions of the expression, to find the aliases of lambdas that it invokes.
        const NameSet & referenced_names;
        const String & session_timezone;
        /// To find the higher-order functions that bind the parameters of a lambda.
        ContextPtr context;
        bool modified = false;
        LambdaParameters lambda_parameters = {};
        /// Whether this is the body of a lambda that is shared by several invocations, or is inside one.
        bool in_shared_lambda = false;
        /// The type that the analyzer gives the alias with the name and the expression, see `getAliasTypes`, if it can be
        /// analyzed against the table.
        std::function<DataTypePtr(const ASTPtr & aliased_expression, const String & name)> resolve_alias_type = {};
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
        /// The body of a lambda with an alias that the expression invokes, such as `f` in
        /// `arrayExists((x -> x = '...') AS f, arr) OR arrayExists(f, utc)`, is shared by the invocations, which may bind its
        /// parameters to other types, so the types of its parameters are not known. They shadow the columns with their names
        /// all the same, as do the parameters of the lambdas inside it, so that the shared body is not rewritten with the type
        /// of a column that one of the invocations may not bind them to.
        const String alias = lambda.tryGetAlias();
        const bool invoked_by_alias = !alias.empty() && data.referenced_names.contains(alias);
        const auto types = invoked_by_alias
            ? DataTypes(parameter_asts.size())
            : getLambdaParameterTypes(
                  function, lambda_position, parameter_asts.size(), data.columns, data.aliases, data.lambda_parameters, data.context);
        const bool shared = data.in_shared_lambda || invoked_by_alias;
        LambdaParameters body_parameters = data.lambda_parameters;
        for (size_t i = 0; i < parameter_asts.size(); ++i)
            if (const auto * identifier = parameter_asts[i]->as<ASTIdentifier>())
                body_parameters[identifier->name()] = LambdaParameter{types[i], shared};

        /// An alias in the body shadows the columns and the outer aliases with its name in the body only. Its type is that of its
        /// expression against the table, as for an alias outside the lambdas, if the expression uses only columns of the table,
        /// and not a parameter of a lambda or another alias with the name of one, such as `identity(event) AS event`, where
        /// `event` names the column. Otherwise, such as for an expression that invokes the alias of a lambda, its type is not
        /// known.
        NameSet body_alias_names;
        std::unordered_map<String, ASTPtr> body_aliased_expressions;
        collectAliases(lambda.arguments->children[1], body_alias_names, &body_aliased_expressions);
        AliasTypes body_aliases = data.aliases;
        for (const auto & name : body_alias_names)
            body_aliases[name] = nullptr;
        if (data.resolve_alias_type)
        {
            NameSet parameter_names;
            for (const auto & [name, _] : body_parameters)
                parameter_names.insert(name);
            NameSet alias_names;
            for (const auto & [name, _] : body_aliases)
                alias_names.insert(name);
            for (const auto & [name, aliased_expression] : body_aliased_expressions)
            {
                NameSet other_alias_names = alias_names;
                other_alias_names.erase(name);
                if (usesOnlyColumns(aliased_expression, data.columns) && !hasIdentifierWithName(aliased_expression, parameter_names)
                    && !hasIdentifierWithName(aliased_expression, other_alias_names))
                    body_aliases[name] = data.resolve_alias_type(aliased_expression, name);
            }
        }

        Data body_data{
            data.columns, body_aliases, data.referenced_names, data.session_timezone, data.context, false, body_parameters, shared,
            data.resolve_alias_type};

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
    NameSet referenced_names;
    for (const auto & expression : expressions)
        collectReferencedNames(expression, referenced_names);

    const ColumnsDescription & columns = storage_snapshot->metadata->getColumns();
    RewriteDateTimeLiteralsMatcher::Data data{columns, aliases, referenced_names, session_timezone, context, false};
    data.resolve_alias_type = [&](const ASTPtr & aliased_expression, const String & name)
    {
        return getAliasTypes({aliased_expression}, {name}, table, storage_snapshot, context).at(name);
    };
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
    NameSet referenced_names;
    collectReferencedNames(root, referenced_names);
    RewriteDateTimeLiteralsMatcher::Data data{columns, aliases, referenced_names, session_timezone, context, false};
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
