#include <Interpreters/MutationsDateTimeLiteralVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageSnapshot.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int TIMEOUT_EXCEEDED;
}

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

/// Wraps a string literal AST with toDateTime('...', 'tz') or toDateTime64('...', scale, 'tz'). An alias of the literal stays on
/// it, so that an identifier that names the alias elsewhere still names the string.
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

const std::unordered_set<String> comparison_functions = {
    "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
};

const std::unordered_set<String> in_functions = {
    "in", "notIn", "globalIn", "globalNotIn",
};

/// How one evaluation of a string literal of the expression reads the string.
struct LiteralUse
{
    /// The `DateTime` or `DateTime64` type without a time zone that the string is read as, or nullptr if it is read otherwise,
    /// or it is not known how.
    DataTypePtr type = nullptr;
    /// The identifier of the expression that the literal is compared with, and its type, if it is.
    const IAST * operand = nullptr;
    DataTypePtr operand_type = nullptr;
    /// Whether it is the cast of an element of the tuple of `IN` to the type of the left argument, which the analyzer makes
    /// without an AST. The type counts only if the left argument of `IN` in the AST is an identifier (see `collectLiteralUses`).
    bool is_in_element_cast = false;
};

/// The evaluations of the string literals of the expression, by the literal.
using LiteralUses = std::unordered_map<const IAST *, std::vector<LiteralUse>>;

/// The string literal of the expression that `node` evaluates, or nullptr.
const IAST * tryGetStringLiteral(const QueryTreeNodePtr & node)
{
    const auto * constant = node ? node->as<ConstantNode>() : nullptr;
    if (!constant || !constant->hasOriginalAST() || !isString(constant->getResultType()))
        return nullptr;
    const auto * literal = constant->getOriginalAST()->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return nullptr;
    return literal;
}

/// The argument `position` of the function with the AST `function_ast`, if the AST has `num_arguments` arguments, or nullptr.
const IAST * tryGetArgumentAST(const ASTPtr & function_ast, size_t position, size_t num_arguments)
{
    const auto * function = function_ast ? function_ast->as<ASTFunction>() : nullptr;
    if (!function || !function->arguments || function->arguments->children.size() != num_arguments)
        return nullptr;
    return function->arguments->children[position].get();
}

/// Collects the evaluations of the string literals in a resolved query tree, as the analyzer binds every identifier: to a
/// column, a subcolumn, an alias, or a parameter of a lambda, which it resolves with each invocation separately, also through
/// the alias of the lambda, in the scope of the identifier and with the settings of the context. A literal is read as a `DateTime`
/// or `DateTime64` without a time zone where it is compared with an identifier of such a type, also as an element of the
/// tuple of `IN` with an identifier on the left, or where it is assigned to a column of such a type. Any other evaluation, such
/// as in a comparison with an expression that is not an identifier, or as an argument of another function, reads it otherwise.
///
/// A converted literal keeps its alias (see `wrapWithTimezone`), so an identifier that names the alias elsewhere, such as `lit`
/// in `event = ('...' AS lit) AND s = lit`, still names the string: such an evaluation, which the analyzer makes with a copy of
/// the literal, does not count, where the AST of the function shows it.
class LiteralUsesCollector
{
public:
    LiteralUses uses;

    /// The expression `ast` that is assigned to a column of type `column_type` by `UPDATE` or `TTL ... GROUP BY ... SET`.
    void collectAssigned(const QueryTreeNodePtr & node, const IAST * ast, const DataTypePtr & column_type)
    {
        const auto * literal = tryGetStringLiteral(node);
        if (!literal)
            collect(node);
        else if (literal == ast)
            uses[literal].push_back({column_type ? getZonelessDateTimeType(column_type) : nullptr});
    }

    void collect(const QueryTreeNodePtr & node)
    {
        if (!node)
            return;

        if (const auto * literal = tryGetStringLiteral(node))
        {
            uses[literal].push_back({});
            return;
        }

        /// A constant that the analyzer folded, such as `concat('2020-01-01', ' 00:00:00')`, keeps its literals in its source.
        if (const auto * constant = node->as<ConstantNode>())
        {
            if (constant->hasSourceExpression())
                collect(constant->getSourceExpression());
            return;
        }

        if (const auto * function = node->as<FunctionNode>())
        {
            if (function->isResolved())
            {
                const auto & name = function->getFunctionName();
                if ((comparison_functions.contains(name) && collectComparison(*function))
                    || (in_functions.contains(name) && collectIn(*function)) || (name == "CAST" && collectInElementCast(*function)))
                    return;
            }
            collectFunction(*function);
            return;
        }

        for (const auto & child : node->getChildren())
            collect(child);
    }

private:
    /// Adds the evaluation `use` of the literal `literal` that is the argument `position` of `function`, unless the AST of the
    /// function shows that it is an identifier that names the alias of the literal there. If the AST does not tell, the literal
    /// is read otherwise.
    void addArgumentUse(const FunctionNode & function, size_t position, const IAST * literal, LiteralUse use)
    {
        const auto * argument_ast = tryGetArgumentAST(function.getOriginalAST(), position, function.getArguments().getNodes().size());
        if (!argument_ast)
            uses[literal].push_back({});
        else if (argument_ast == literal)
            uses[literal].push_back(std::move(use));
    }

    void collectFunction(const FunctionNode & function)
    {
        const auto & arguments = function.getArguments().getNodes();
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (const auto * literal = tryGetStringLiteral(arguments[i]))
                addArgumentUse(function, i, literal, {});
            else
                collect(arguments[i]);
        }
        for (const auto & child : function.getChildren())
            if (child.get() != &function.getArguments())
                collect(child);
    }

    /// `event = '...'` or `'...' = event`.
    bool collectComparison(const FunctionNode & function)
    {
        const auto & arguments = function.getArguments().getNodes();
        if (arguments.size() != 2)
            return false;

        for (size_t i = 0; i < 2; ++i)
        {
            const auto * literal = tryGetStringLiteral(arguments[i]);
            if (!literal)
            {
                collect(arguments[i]);
                continue;
            }
            const auto & other = arguments[1 - i];
            const auto * operand = tryGetArgumentAST(function.getOriginalAST(), 1 - i, 2);
            if (operand && operand->as<ASTIdentifier>())
                addArgumentUse(function, i, literal, {getZonelessDateTimeType(other->getResultType()), operand, other->getResultType()});
            else
                addArgumentUse(function, i, literal, {});
        }
        return true;
    }

    /// `event IN tuple('...', ...)`. A tuple of literals only, which the parser folds into one literal, is left as it is.
    bool collectIn(const FunctionNode & function)
    {
        const auto & arguments = function.getArguments().getNodes();
        if (arguments.size() != 2)
            return false;

        const auto * operand = tryGetArgumentAST(function.getOriginalAST(), 0, 2);
        QueryTreeNodePtr tuple = arguments[1];
        if (const auto * constant = tuple->as<ConstantNode>(); constant && constant->hasSourceExpression())
            tuple = constant->getSourceExpression();
        const auto * tuple_function = tuple->as<FunctionNode>();
        if (!operand || !operand->as<ASTIdentifier>() || !tuple_function || tuple_function->getFunctionName() != "tuple")
            return false;

        collect(arguments[0]);
        const auto type = getZonelessDateTimeType(arguments[0]->getResultType());
        const auto & elements = tuple_function->getArguments().getNodes();
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (const auto * literal = tryGetStringLiteral(elements[i]))
                addArgumentUse(*tuple_function, i, literal, {type, operand, arguments[0]->getResultType()});
            else
                collect(elements[i]);
        }
        return true;
    }

    /// The analyzer rewrites `event IN tuple('...', x)` with an element that is not a constant into `has([...], event)` and casts
    /// each element to the type of `event`, without an AST of the cast or of `has`. The literal is read with that type there, but
    /// as with a comparison, only if `event` is an identifier, which only the AST shows.
    bool collectInElementCast(const FunctionNode & function)
    {
        const auto & arguments = function.getArguments().getNodes();
        if (function.hasOriginalAST() || arguments.size() != 2)
            return false;
        const auto * literal = tryGetStringLiteral(arguments[0]);
        if (!literal)
            return false;
        uses[literal].push_back({getZonelessDateTimeType(function.getResultType()), nullptr, nullptr, /*is_in_element_cast=*/true});
        collect(arguments[1]);
        return true;
    }
};

/// An expression whose string literals are rewritten, with the type of the column that it is assigned to, if it is.
struct Expression
{
    ASTPtr ast;
    DataTypePtr assigned_column_type = nullptr;
};

/// Adds the nodes of `copy`, a copy of `original`, to `originals`, with the nodes of `original` they copy. `IAST::clone` copies
/// the children in their order, so the nodes are matched by their positions. A node that does not match, by its kind or by the
/// number of its children, is not matched with its children, so a literal in it is not converted.
void matchCopiedAST(const IAST & copy, const IAST & original, std::unordered_map<const IAST *, const IAST *> & originals)
{
    if (copy.getID() != original.getID())
        return;
    originals.emplace(&copy, &original);
    if (copy.children.size() != original.children.size())
        return;
    for (size_t i = 0; i < copy.children.size(); ++i)
        matchCopiedAST(*copy.children[i], *original.children[i], originals);
}

/// Adds the parents of the nodes of `ast` to `parents`.
void collectParents(const IAST & ast, std::unordered_map<const IAST *, const IAST *> & parents)
{
    for (const auto & child : ast.children)
    {
        parents.emplace(child.get(), &ast);
        collectParents(*child, parents);
    }
}

/// The function whose argument `node` is, according to `parents`, or nullptr.
const ASTFunction * tryGetFunctionOfArgument(const IAST * node, const std::unordered_map<const IAST *, const IAST *> & parents)
{
    const auto arguments = parents.find(node);
    if (arguments == parents.end())
        return nullptr;
    const auto function = parents.find(arguments->second);
    if (function == parents.end())
        return nullptr;
    const auto * function_ast = function->second->as<ASTFunction>();
    if (!function_ast || function_ast->arguments.get() != arguments->second)
        return nullptr;
    return function_ast;
}

/// Whether `literal` is an element of the tuple of `IN` whose left argument is an identifier, such as in
/// `event IN tuple('...', x)`, according to `parents`.
bool isElementOfInWithIdentifier(const IAST * literal, const std::unordered_map<const IAST *, const IAST *> & parents)
{
    const auto * tuple = tryGetFunctionOfArgument(literal, parents);
    if (!tuple || tuple->name != "tuple")
        return false;
    const auto * in = tryGetFunctionOfArgument(tuple, parents);
    if (!in || !in_functions.contains(in->name) || in->arguments->children.size() != 2)
        return false;
    return in->arguments->children[0]->as<ASTIdentifier>() && in->arguments->children[1].get() == tuple;
}

/// Analyzes `expressions` against `table_node` with the settings of `context`, as `MutationsInterpreter` resolves the expressions
/// of a mutation together, and collects the evaluations of their literals.
LiteralUses collectLiteralUses(
    const std::vector<Expression> & expressions, const TableExpressionNodePtr & table_node, const ContextPtr & context)
{
    auto list = make_intrusive<ASTExpressionList>();
    for (const auto & expression : expressions)
        list->children.push_back(expression.ast);

    auto query_tree = buildQueryTree(list, context);

    /// `buildQueryTree` builds the nodes from a copy of the AST, which they refer to, so the literals and the identifiers of the
    /// copy are matched with those of `expressions`. A matcher, such as `*`, may be expanded to several nodes, so then the nodes
    /// are not matched with the expressions.
    std::unordered_map<const IAST *, const IAST *> originals;
    std::unordered_map<const IAST *, const IAST *> parents;
    ASTs copies;
    const auto & built_nodes = query_tree->as<const ListNode &>().getNodes();
    if (built_nodes.size() == expressions.size())
    {
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            copies.push_back(built_nodes[i]->getOriginalAST());
            if (copies.back())
            {
                matchCopiedAST(*copies.back(), *expressions[i].ast, originals);
                collectParents(*copies.back(), parents);
            }
        }
    }

    QueryAnalyzer(/*only_analyze=*/true).resolve(query_tree, table_node, context);

    LiteralUsesCollector collector;
    const auto & nodes = query_tree->as<const ListNode &>().getNodes();
    const bool nodes_match_expressions = nodes.size() == expressions.size() && copies.size() == expressions.size();
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        if (nodes_match_expressions)
            collector.collectAssigned(nodes[i], copies[i].get(), expressions[i].assigned_column_type);
        else
            collector.collect(nodes[i]);
    }

    LiteralUses uses;
    for (auto & [literal, literal_uses] : collector.uses)
    {
        const auto original = originals.find(literal);
        if (original == originals.end())
            continue;
        for (auto & use : literal_uses)
        {
            if (use.is_in_element_cast && !isElementOfInWithIdentifier(literal, parents))
                use.type = nullptr;
            const auto original_operand = use.operand ? originals.find(use.operand) : originals.end();
            use.operand = original_operand != originals.end() ? original_operand->second : nullptr;
        }
        uses.emplace(original->second, std::move(literal_uses));
    }
    return uses;
}

/// How all the evaluations of a literal read it: the type, and the identifier it is compared with and its type, if they are the
/// same for all of them. Returns std::nullopt if they read it otherwise, or with different types.
struct Reading
{
    DataTypePtr type;
    const IAST * operand = nullptr;
    DataTypePtr operand_type;
};

std::optional<Reading> tryGetCommonReading(const std::vector<LiteralUse> & uses)
{
    if (uses.empty() || !uses.front().type)
        return std::nullopt;
    Reading reading{uses.front().type, uses.front().operand, uses.front().operand_type};
    for (const auto & use : uses)
    {
        if (!use.type || !use.type->equals(*reading.type))
            return std::nullopt;
        if (use.operand != reading.operand)
            reading.operand = nullptr;
        if (!use.operand_type || !reading.operand_type || !use.operand_type->equals(*reading.operand_type))
            reading.operand_type = nullptr;
    }
    return reading;
}

/// The conversion of a string literal.
struct Conversion
{
    DataTypePtr type;
    /// If the identifier that the literal is compared with has another type with the other value of `prefer_column_name_to_alias`,
    /// the identifier, the name of that type, and the type the literal is read as then.
    const IAST * operand = nullptr;
    String alternative_operand_type_name = {};
    DataTypePtr alternative_type = nullptr;
};

/// The conversions of the literals that every evaluation reads as the same `DateTime` or `DateTime64` without a time zone, in
/// `uses`, and in `alternative_uses` with the other value of `prefer_column_name_to_alias` if it is given.
///
/// A literal that is evaluated in several places, such as in the body of a lambda that is invoked through its alias with
/// arrays of several types, is converted only if all of them read it the same way. If the identifier that it is compared with
/// names an alias with one value of the setting and the column with its name with the other one, and their types differ, such
/// as `toDateTime64(event, 3) AS event` of a `DateTime` column, the string is read with each type, and the one of the type that
/// the identifier has where the expression is evaluated is taken: `if(toTypeName(event) = 'DateTime', toDateTime('...', 'tz'),
/// toDateTime64('...', 3, 'tz'))`. So it is read as the comparison of the string reads it, with the same range and rounding.
std::unordered_map<const IAST *, Conversion> getConversions(const LiteralUses & uses, const std::optional<LiteralUses> & alternative_uses)
{
    std::unordered_map<const IAST *, Conversion> conversions;
    for (const auto & [literal, literal_uses] : uses)
    {
        const auto reading = tryGetCommonReading(literal_uses);
        if (!reading)
            continue;

        /// A literal that is not evaluated with the other value of the setting is read only as with this one.
        const auto alternative_it = alternative_uses ? alternative_uses->find(literal) : LiteralUses::const_iterator{};
        if (!alternative_uses || alternative_it == alternative_uses->end())
        {
            conversions.emplace(literal, Conversion{reading->type});
            continue;
        }

        const auto alternative_reading = tryGetCommonReading(alternative_it->second);
        if (!alternative_reading)
            continue;
        if (alternative_reading->type->equals(*reading->type))
        {
            conversions.emplace(literal, Conversion{reading->type});
            continue;
        }
        if (!reading->operand || reading->operand != alternative_reading->operand || !reading->operand_type
            || !alternative_reading->operand_type || reading->operand_type->getName() == alternative_reading->operand_type->getName())
            continue;
        conversions.emplace(
            literal, Conversion{reading->type, reading->operand, alternative_reading->operand_type->getName(), alternative_reading->type});
    }
    return conversions;
}

ASTPtr convertLiteral(const ASTPtr & literal_ast, const Conversion & conversion, const String & timezone)
{
    if (!conversion.operand)
        return wrapWithTimezone(literal_ast, conversion.type, timezone);
    auto names_alternative = makeASTFunction(
        "equals",
        makeASTFunction("toTypeName", conversion.operand->clone()),
        make_intrusive<ASTLiteral>(Field(conversion.alternative_operand_type_name)));
    auto read_for_alternative = wrapWithTimezone(literal_ast->clone(), conversion.alternative_type, timezone);
    auto read_for_this = wrapWithTimezone(literal_ast, conversion.type, timezone);
    return makeASTFunction("if", std::move(names_alternative), std::move(read_for_alternative), std::move(read_for_this));
}

/// Replaces the literals of `ast` that `conversions` converts, which are arguments of functions or values of assignments. Returns
/// whether any was replaced.
bool convertLiterals(const ASTPtr & ast, const std::unordered_map<const IAST *, Conversion> & conversions, const String & timezone)
{
    bool converted = false;
    const bool replaces_children = ast->as<ASTExpressionList>() || ast->as<ASTAssignment>();
    for (auto & child : ast->children)
    {
        if (const auto it = conversions.find(child.get()); replaces_children && it != conversions.end())
        {
            child = convertLiteral(child, it->second, timezone);
            converted = true;
        }
        else
        {
            converted |= convertLiterals(child, conversions, timezone);
        }
    }
    /// The assignments of `TTL ... GROUP BY ... SET` are not children of the element.
    if (const auto * ttl_element = ast->as<ASTTTLElement>())
        for (const auto & assignment : ttl_element->group_by_assignments)
            converted |= convertLiterals(assignment, conversions, timezone);
    return converted;
}

/// Whether `ast` defines an alias, in any scope.
bool hasAlias(const ASTPtr & ast)
{
    if (!ast->tryGetAlias().empty())
        return true;
    return std::ranges::any_of(ast->children, [](const ASTPtr & child) { return hasAlias(child); });
}

/// The type of the column `name` if `columns` has it, or nullptr.
DataTypePtr tryGetColumnType(const String & name, const ColumnsDescription & columns)
{
    const auto * column = columns.tryGet(name);
    return column ? column->type : nullptr;
}

/// Adds the expressions of the `TTL` elements in `ast` to `expressions`: the expression of the `TTL`, its `WHERE`, and the values
/// of the assignments of its `GROUP BY`. The keys of the `GROUP BY` are a prefix of the primary key, which has no literal to convert,
/// and the validation of the `TTL` reports a wrong one.
void collectTTLExpressions(const ASTPtr & ast, const ColumnsDescription & columns, std::vector<Expression> & expressions)
{
    const auto * ttl_element = ast->as<ASTTTLElement>();
    if (!ttl_element)
    {
        for (const auto & child : ast->children)
            collectTTLExpressions(child, columns, expressions);
        return;
    }

    if (auto ttl = ttl_element->ttl())
        expressions.push_back({ttl});
    if (auto where = ttl_element->where())
        expressions.push_back({where});
    for (const auto & assignment_ast : ttl_element->group_by_assignments)
        if (const auto * assignment = assignment_ast->as<ASTAssignment>())
            expressions.push_back({assignment->expression(), tryGetColumnType(assignment->column_name, columns)});
}

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
    const ColumnsDescription & columns = storage_snapshot->metadata->getColumns();

    /// The predicate and the values of the assignments, which `MutationsInterpreter` resolves together.
    std::vector<Expression> expressions;
    if (new_command.predicate)
        expressions.push_back({new_command.predicate->ptr()});
    if (new_command.update_assignments)
        for (const auto & child : new_command.update_assignments->children)
            if (const auto * assignment = child->as<ASTAssignment>())
                expressions.push_back({assignment->expression(), tryGetColumnType(assignment->column_name, columns)});
    if (expressions.empty())
        return nullptr;

    /// The expressions are only analyzed and nothing is read, so the table is not locked.
    const auto table_node = std::make_shared<TableNode>(table, TableLockHolder{}, storage_snapshot);
    const auto uses = collectLiteralUses(expressions, table_node, Context::createCopy(context));

    /// The mutation runs with the settings of the server or of the session, depending on the engine, which may differ from those
    /// of this query. With `prefer_column_name_to_alias` there, an identifier with the name of an alias names the column with
    /// that name instead, if there is one, so the expressions are analyzed with the other value of the setting too, if they have
    /// an alias. If they cannot be analyzed with it, the mutation fails where it runs with it whatever its literals are, so only
    /// the evaluations with the settings of this query matter.
    std::optional<LiteralUses> alternative_uses;
    if (std::ranges::any_of(expressions, [](const Expression & expression) { return hasAlias(expression.ast); }))
    {
        auto alternative_context = Context::createCopy(context);
        alternative_context->setSetting(
            "prefer_column_name_to_alias", Field(!context->getSettingsRef()[Setting::prefer_column_name_to_alias]));
        try
        {
            alternative_uses = collectLiteralUses(expressions, table_node, alternative_context);
        }
        catch (const Exception & e)
        {
            /// Only a failure to analyze the expressions tells that they do not resolve with the other value of the setting.
            if (e.code() == ErrorCodes::LOGICAL_ERROR || e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED
                || e.code() == ErrorCodes::QUERY_WAS_CANCELLED || e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
                throw;
        }
    }

    /// The root of the predicate is not a literal that is compared or assigned, so it stays the same.
    const auto conversions = getConversions(uses, alternative_uses);
    bool converted = false;
    if (new_command.predicate)
        converted |= convertLiterals(new_command.predicate->ptr(), conversions, session_timezone);
    if (new_command.update_assignments)
        converted |= convertLiterals(new_command.update_assignments->ptr(), conversions, session_timezone);

    if (!converted)
        return nullptr;
    return query;
}

bool rewriteDateTimeLiteralsWithTimezone(
    const ASTPtr & expression, const ColumnsDescription & columns, const String & session_timezone, const ContextPtr & context)
{
    if (session_timezone.empty() || !expression)
        return false;

    std::vector<Expression> expressions;
    collectTTLExpressions(expression, columns, expressions);
    if (expressions.empty())
        return false;

    /// A `TTL` does not allow aliases, so it is analyzed with the settings of this query only.
    auto execution_context = Context::createCopy(context);
    const auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, columns);
    const auto table_node = std::make_shared<TableNode>(storage, execution_context);
    const auto conversions = getConversions(collectLiteralUses(expressions, table_node, execution_context), std::nullopt);

    /// The visitor replaces only the arguments of the functions and the values of the assignments, so the root of the
    /// expression stays the same.
    return convertLiterals(expression, conversions, session_timezone);
}

}
