#include <Analyzer/Resolve/FunctionCompositionRewrite.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <base/defines.h>

#include <algorithm>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// A lambda with more arguments makes no sense: a placeholder number this large is a typo.
constexpr size_t max_placeholder_number = 128;

/// The preferred name of an argument of a fused lambda. Reserved-looking on purpose: the body
/// of the right operand is resolved with these names in scope, so they should not collide with
/// a column a user could reasonably reference from there. A collision is still possible, and
/// `fuseCompositionToLambda` disambiguates the name in that case.
String composedArgumentName(size_t argument_number)
{
    return "__composed_arg_" + std::to_string(argument_number);
}

String placeholderName(size_t placeholder_number)
{
    return "_" + std::to_string(placeholder_number);
}

/// Returns 0 for the anonymous placeholder `_`, N for a numbered placeholder `_N`
/// (a positive integer without leading zeros), and nothing for any other name.
std::optional<size_t> parsePlaceholderName(const String & name)
{
    if (name.empty() || name.front() != '_')
        return {};

    if (name.size() == 1)
        return 0;

    if (name[1] < '1' || name[1] > '9')
        return {};

    size_t number = 0;
    for (size_t i = 1; i < name.size(); ++i)
    {
        if (name[i] < '0' || name[i] > '9')
            return {};
        number = number * 10 + (name[i] - '0');
        if (number > max_placeholder_number)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument placeholder {} is not allowed: at most {} lambda arguments are supported",
                backQuote(name), max_placeholder_number);
    }

    return number;
}

std::optional<size_t> tryGetPlaceholder(const IQueryTreeNode & node)
{
    const auto * identifier_node = node.as<IdentifierNode>();
    if (!identifier_node)
        return {};

    /// A compound identifier whose first part is a placeholder is a member access on the
    /// placeholder, e.g. `_1.a`. It references the same lambda argument.
    return parsePlaceholderName(identifier_node->getIdentifier().at(0));
}

struct PlaceholderCollector
{
    /// Numbers of the numbered placeholders that occur free anywhere in the expression.
    std::set<size_t> numbered;
    /// Free occurrences of the anonymous placeholder `_` anywhere in the expression.
    size_t anonymous_occurrences = 0;

    void collect(const QueryTreeNodePtr & node, NameSet & bound_names)
    {
        if (!node)
            return;

        switch (node->getNodeType())
        {
            case QueryTreeNodeType::IDENTIFIER:
            {
                const auto & name = node->as<IdentifierNode &>().getIdentifier().at(0);
                if (bound_names.contains(name))
                    return;
                if (auto placeholder = parsePlaceholderName(name))
                {
                    if (*placeholder == 0)
                        ++anonymous_occurrences;
                    else
                        numbered.insert(*placeholder);
                }
                return;
            }
            case QueryTreeNodeType::LAMBDA:
            {
                /// The arguments of a nested lambda shadow placeholders with the same names
                /// inside its body.
                const auto & lambda_node = node->as<LambdaNode &>();
                Names newly_bound;
                for (const auto & argument_name : lambda_node.getArguments().getNames())
                    if (bound_names.emplace(argument_name).second)
                        newly_bound.push_back(argument_name);

                collect(lambda_node.getExpression(), bound_names);

                for (const auto & argument_name : newly_bound)
                    bound_names.erase(argument_name);
                return;
            }
            case QueryTreeNodeType::FUNCTION:
            {
                /// Placeholders inside a nested composition belong to its own operands.
                if (isFunctionComposition(*node))
                    return;
                break;
            }
            case QueryTreeNodeType::QUERY:
            case QueryTreeNodeType::UNION:
            {
                /// Placeholders are not supported inside subqueries; identifiers there are
                /// resolved as usual.
                return;
            }
            default:
                break;
        }

        for (const auto & child : node->getChildren())
            collect(child, bound_names);
    }
};

/// The names a scope binds, split by what an occurrence of the name can be. Both kinds matter:
/// substitution replaces a bare identifier `x` as well as the member access `x.a`, so a name
/// that binds only as a qualifier still hides the compound form.
struct BoundNames
{
    /// Names that bind a bare, one-part expression identifier: the arguments of a lambda, the
    /// aliases of expressions, and the columns a subquery in the join tree exposes.
    NameSet expressions;
    /// Names that bind only the first part of a compound identifier: the alias of a table
    /// expression and the name of a CTE. Once column lookup misses,
    /// `IdentifierResolver::tryBindIdentifierToTableExpression` rejects a one-part expression
    /// lookup, so `FROM (SELECT 1 AS z) AS x` makes `x.z` local but leaves a bare `x` free.
    NameSet qualifiers;
};

/// The names of the columns a subquery in a join tree exposes to the query that selects from
/// it. They are bound names at that query level, even though the expressions behind them live
/// in another scope. Only names that are evident lexically are collected: an explicit alias, or
/// the column name of a bare identifier projection.
void collectProjectionNames(const QueryTreeNodePtr & node, BoundNames & bound_names)
{
    if (const auto * query_node = node->as<QueryNode>())
    {
        for (const auto & projection_node : query_node->getProjection().getNodes())
        {
            if (projection_node->hasAlias())
                bound_names.expressions.insert(projection_node->getAlias());
            else if (const auto * identifier_node = projection_node->as<IdentifierNode>())
                bound_names.expressions.insert(identifier_node->getIdentifier().getParts().back());
        }
    }
    else if (const auto * union_node = node->as<UnionNode>())
    {
        for (const auto & union_query_node : union_node->getQueries().getNodes())
            collectProjectionNames(union_query_node, bound_names);
    }
}

/// The names a join tree binds: the aliases of its table expressions, which are qualifiers only,
/// and the columns exposed by the subqueries it selects from, which bind a bare identifier. The
/// columns of a real table cannot be known here — that needs the catalog — which is the
/// conservative case documented at `subqueryReferencesIdentifier`.
void collectNamesBoundInJoinTree(const QueryTreeNodePtr & node, BoundNames & bound_names)
{
    if (!node)
        return;

    if (node->hasAlias())
        bound_names.qualifiers.insert(node->getAlias());

    const auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        collectProjectionNames(node, bound_names);
        return;
    }

    for (const auto & child : node->getChildren())
        collectNamesBoundInJoinTree(child, bound_names);
}

/// Collect every name bound at the level of one query: aliases (`SELECT 1 AS x`), the names of
/// the CTEs it defines, and what its join tree binds. Such a name is visible in the whole query,
/// so the walk descends through it, but it stops at a nested query (which has its own level) and
/// at a lambda (whose arguments are visible only inside its own body).
void collectNamesBoundAtQueryLevel(const QueryTreeNodePtr & node, BoundNames & bound_names, bool is_root)
{
    if (!node)
        return;

    if (is_root)
    {
        /// An alias on the query node itself belongs to the enclosing scope, not to this level.
        /// The join tree is collected separately: what it binds is not what an alias binds.
        QueryTreeNodePtr join_tree_node;
        if (const auto * root_query_node = node->as<QueryNode>())
        {
            join_tree_node = root_query_node->getJoinTreeNode();
            collectNamesBoundInJoinTree(join_tree_node, bound_names);
        }

        for (const auto & child : node->getChildren())
            if (child != join_tree_node)
                collectNamesBoundAtQueryLevel(child, bound_names, false /*is_root*/);

        return;
    }

    if (node->hasAlias())
        bound_names.expressions.insert(node->getAlias());

    if (const auto * query_node = node->as<QueryNode>())
    {
        if (query_node->isCTE())
            bound_names.qualifiers.insert(query_node->getCTEName());
        return;
    }

    if (const auto * union_node = node->as<UnionNode>())
    {
        if (union_node->isCTE())
            bound_names.qualifiers.insert(union_node->getCTEName());
        return;
    }

    if (node->getNodeType() == QueryTreeNodeType::LAMBDA)
        return;

    for (const auto & child : node->getChildren())
        collectNamesBoundAtQueryLevel(child, bound_names, false /*is_root*/);
}

/// Whether a subquery references the name from the outside, so that a substitution would have
/// to descend into it. The walk is scope sensitive: a binding suppresses the name only inside
/// the scope that introduces it — a lambda argument only inside the body of that lambda, an
/// alias or a CTE name only inside the query that defines it. A nested binder therefore does
/// not hide a free occurrence of the same name elsewhere in the subquery.
///
/// Whether a bare identifier resolves to a column of a table inside the subquery cannot be
/// decided lexically: it needs the catalog, which is not available in this rewrite. So a name
/// that is not bound by any of the constructs above is conservatively treated as referenced,
/// and the composition fails cleanly instead of substituting into the subquery.
bool subqueryReferencesIdentifier(const QueryTreeNodePtr & node, const String & name, BoundNames bound_names)
{
    if (!node)
        return false;

    switch (node->getNodeType())
    {
        case QueryTreeNodeType::IDENTIFIER:
        {
            const auto & identifier = node->as<IdentifierNode &>().getIdentifier();
            if (identifier.at(0) != name)
                return false;
            if (bound_names.expressions.contains(name))
                return false;
            /// A table alias or a CTE name binds `x.a` but not a bare `x`.
            return !(identifier.isCompound() && bound_names.qualifiers.contains(name));
        }
        case QueryTreeNodeType::LAMBDA:
        {
            const auto & lambda_node = node->as<LambdaNode &>();
            const auto & argument_names = lambda_node.getArguments().getNames();
            if (std::find(argument_names.begin(), argument_names.end(), name) != argument_names.end())
                return false;

            return subqueryReferencesIdentifier(lambda_node.getExpression(), name, bound_names);
        }
        case QueryTreeNodeType::QUERY:
        case QueryTreeNodeType::UNION:
        {
            collectNamesBoundAtQueryLevel(node, bound_names, true /*is_root*/);
            if (bound_names.expressions.contains(name))
                return false;
            break;
        }
        default:
            break;
    }

    for (const auto & child : node->getChildren())
        if (subqueryReferencesIdentifier(child, name, bound_names))
            return true;

    return false;
}

bool subqueryReferencesIdentifier(const QueryTreeNodePtr & node, const String & name)
{
    return subqueryReferencesIdentifier(node, name, BoundNames{});
}



/// Every name that occurs as an identifier, a lambda argument, or an alias anywhere in the
/// subtree. A name outside of this set cannot capture anything in it and cannot be captured
/// by it, so it is safe to use as the argument name of a synthesized lambda.
void collectUsedNames(const QueryTreeNodePtr & node, NameSet & used_names)
{
    if (!node)
        return;

    if (node->hasAlias())
        used_names.insert(node->getAlias());

    if (const auto * identifier_node = node->as<IdentifierNode>())
        used_names.insert(identifier_node->getIdentifier().at(0));
    else if (const auto * lambda_node = node->as<LambdaNode>())
        for (const auto & argument_name : lambda_node->getArguments().getNames())
            used_names.insert(argument_name);

    for (const auto & child : node->getChildren())
        collectUsedNames(child, used_names);
}

/// Replace free occurrences of the identifier `name` in the expression with (a clone of) the
/// `replacement` node. A member access on the identifier, e.g. `t.a.b` for `name` = `t`,
/// becomes the corresponding `tupleElement` chain over the replacement.
void substituteIdentifier(QueryTreeNodePtr & node, const String & name, const QueryTreeNodePtr & replacement)
{
    if (!node)
        return;

    switch (node->getNodeType())
    {
        case QueryTreeNodeType::IDENTIFIER:
        {
            const auto & parts = node->as<IdentifierNode &>().getIdentifier().getParts();
            if (parts.at(0) != name)
                return;

            auto result = replacement->clone();
            for (size_t i = 1; i < parts.size(); ++i)
            {
                auto tuple_element = std::make_shared<FunctionNode>("tupleElement");
                tuple_element->getArguments().getNodes().push_back(std::move(result));
                tuple_element->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(parts[i]));
                result = std::move(tuple_element);
            }

            node = std::move(result);
            return;
        }
        case QueryTreeNodeType::LAMBDA:
        {
            /// A nested lambda that rebinds the name shadows it.
            auto & lambda_node = node->as<LambdaNode &>();
            const auto & argument_names = lambda_node.getArguments().getNames();
            if (std::find(argument_names.begin(), argument_names.end(), name) != argument_names.end())
                return;

            substituteIdentifier(lambda_node.getExpression(), name, replacement);
            return;
        }
        case QueryTreeNodeType::FUNCTION:
        {
            /// Placeholders inside a nested composition belong to its own operands: a nested
            /// composition rebinds them like a shadowing lambda, so a placeholder-named
            /// argument is never substituted into one. Any other name is an ordinary capture.
            if (isFunctionComposition(*node) && parsePlaceholderName(name))
                return;
            break;
        }
        case QueryTreeNodeType::QUERY:
        case QueryTreeNodeType::UNION:
        {
            if (subqueryReferencesIdentifier(node, name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "A subquery inside an operand of the function composition operator `|` references {}, "
                    "which the composition would have to substitute into the subquery. This is not supported",
                    backQuote(name));
            return;
        }
        default:
            break;
    }

    for (auto & child : node->getChildren())
        substituteIdentifier(child, name, replacement);
}

[[noreturn]] void throwInvalidOperand(const QueryTreeNodePtr & operand)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Each operand of the function composition operator `|` must be a function: a lambda, "
        "the name of a function, or an expression with argument placeholders `_`, `_1`, `_2`, ... "
        "Got {}. If you meant bitwise OR, use the function bitOr",
        operand->formatASTForErrorMessage());
}

QueryTreeNodePtr normalizeOperandToLambda(
    const QueryTreeNodePtr & operand,
    bool is_left_operand,
    const ResolveIdentifierOperand & resolve_identifier_operand)
{
    switch (operand->getNodeType())
    {
        case QueryTreeNodeType::LAMBDA:
            return operand->clone();
        case QueryTreeNodeType::FUNCTION:
        {
            const auto & function_operand = operand->as<FunctionNode &>();
            if (isFunctionComposition(*operand))
                return fuseCompositionToLambda(function_operand, resolve_identifier_operand);

            if (collectFreePlaceholderNames(operand).empty())
                throwInvalidOperand(operand);

            return liftPlaceholdersToLambda(operand);
        }
        case QueryTreeNodeType::IDENTIFIER:
        {
            /// The right operand is applied to exactly one value, so a variadic function name
            /// (e.g. toString) can be composed on the right; on the left the arity must be
            /// inferable from the function itself.
            std::optional<size_t> required_arity;
            if (!is_left_operand)
                required_arity = 1;

            /// A name bound in the query keeps priority over the placeholder syntax, so the
            /// scoped resolution comes first: in
            /// `WITH (x -> x + 10) AS _1 SELECT arrayMap(_1 | toString, [1])`
            /// the operand is the bound lambda, not the identity function.
            if (auto lambda = resolve_identifier_operand(operand->as<IdentifierNode &>(), required_arity))
                return lambda;

            /// A bare placeholder is the identity function: `_1 | plus(_, 1)` is the same as
            /// `(x -> x) | plus(_, 1)`.
            if (tryGetPlaceholder(*operand))
                return liftPlaceholdersToLambda(operand);

            throwInvalidOperand(operand);
        }
        default:
            throwInvalidOperand(operand);
    }
}

}

bool isFunctionComposition(const IQueryTreeNode & node)
{
    const auto * function_node = node.as<FunctionNode>();
    return function_node && function_node->isOperator() && function_node->getFunctionName() == function_composition_name;
}

NameSet collectFreePlaceholderNames(const QueryTreeNodePtr & node)
{
    PlaceholderCollector collector;
    NameSet bound_names;
    collector.collect(node, bound_names);

    NameSet result;
    if (collector.anonymous_occurrences > 0)
        result.insert("_");
    for (const auto placeholder_number : collector.numbered)
        result.insert(placeholderName(placeholder_number));
    return result;
}

QueryTreeNodePtr liftPlaceholdersToLambda(const QueryTreeNodePtr & node)
{
    PlaceholderCollector collector;
    NameSet bound_names;
    collector.collect(node, bound_names);

    if (collector.anonymous_occurrences > 0 && !collector.numbered.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Mixing the anonymous argument placeholder `_` with numbered placeholders `_N` "
            "in one expression is ambiguous: {}. Use only numbered placeholders",
            node->formatASTForErrorMessage());

    auto body = node->clone();
    size_t lambda_arity = 0;

    if (collector.anonymous_occurrences > 0)
    {
        /// Anonymous placeholders must be direct arguments of the call: at a deeper level
        /// their left-to-right numbering would not be evident from the query text. The whole
        /// expression being the placeholder itself is the identity function.
        auto * body_function = body->as<FunctionNode>();
        size_t direct_anonymous_occurrences = 0;
        if (auto body_placeholder = tryGetPlaceholder(*body); body_placeholder && *body_placeholder == 0)
        {
            if (body->as<IdentifierNode &>().getIdentifier().isCompound())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Member access on the anonymous argument placeholder `_` is not supported: {}. "
                    "Use a numbered placeholder: `_1.name`",
                    node->formatASTForErrorMessage());

            direct_anonymous_occurrences = 1;
            body = std::make_shared<IdentifierNode>(Identifier(placeholderName(1)));
        }
        else if (body_function)
        {
            for (auto & argument : body_function->getArguments().getNodes())
            {
                auto placeholder = tryGetPlaceholder(*argument);
                if (placeholder && *placeholder == 0)
                {
                    if (argument->as<IdentifierNode &>().getIdentifier().isCompound())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Member access on the anonymous argument placeholder `_` is not supported: {}. "
                            "Use a numbered placeholder: `_1.name`",
                            node->formatASTForErrorMessage());

                    ++direct_anonymous_occurrences;
                    argument = std::make_shared<IdentifierNode>(Identifier(placeholderName(direct_anonymous_occurrences)));
                }
            }
        }

        if (direct_anonymous_occurrences != collector.anonymous_occurrences)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The anonymous argument placeholder `_` must be a direct argument of the call: {}. "
                "Use numbered placeholders `_1`, `_2`, ... at deeper levels",
                node->formatASTForErrorMessage());

        lambda_arity = direct_anonymous_occurrences;
    }
    else
    {
        chassert(!collector.numbered.empty());
        lambda_arity = *collector.numbered.rbegin();
    }

    Names lambda_argument_names;
    lambda_argument_names.reserve(lambda_arity);
    for (size_t i = 1; i <= lambda_arity; ++i)
        lambda_argument_names.push_back(placeholderName(i));

    auto lambda_arguments = std::make_shared<LambdaArgumentsNode>(std::move(lambda_argument_names));
    auto lambda = std::make_shared<LambdaNode>(std::move(lambda_arguments), std::move(body), false /*is_operator*/);
    lambda->setAlias(node->getAlias());
    return lambda;
}

QueryTreeNodePtr fuseCompositionToLambda(const FunctionNode & compose_node, const ResolveIdentifierOperand & resolve_identifier_operand)
{
    if (!compose_node.getParameters().getNodes().empty() || compose_node.isWindowFunction()
        || compose_node.getNullsAction() != NullsAction::EMPTY)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The function composition operator `|` cannot have parameters, a window, or a NULLS action: {}",
            compose_node.formatASTForErrorMessage());

    const auto & operands = compose_node.getArguments().getNodes();
    if (operands.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The function composition operator `|` takes exactly 2 operands, got {}: {}",
            operands.size(), compose_node.formatASTForErrorMessage());

    auto left = normalizeOperandToLambda(operands[0], true /*is_left_operand*/, resolve_identifier_operand);
    auto right = normalizeOperandToLambda(operands[1], false /*is_left_operand*/, resolve_identifier_operand);

    auto & left_lambda = left->as<LambdaNode &>();
    auto & right_lambda = right->as<LambdaNode &>();

    const auto & right_argument_names = right_lambda.getArguments().getNames();
    if (right_argument_names.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The right operand of the function composition operator `|` must be a function of one argument, "
            "got {} arguments: {}",
            right_argument_names.size(), operands[1]->formatASTForErrorMessage());

    /// Rename the arguments of the left lambda to fresh names. Without the renaming a column
    /// referenced from the body of the right lambda could be captured by an argument of the
    /// left lambda with the same name, silently changing its meaning.
    ///
    /// The names have to be fresh for both operands: a name that already occurs in either body
    /// would introduce the very capture the renaming avoids — an outer column named
    /// `__composed_arg_1` and referenced from the right operand would bind to the argument of
    /// the fused lambda instead. So the preferred name is disambiguated with a counter until it
    /// occurs in neither operand.
    NameSet used_names;
    collectUsedNames(left, used_names);
    collectUsedNames(right, used_names);

    const auto left_argument_names = left_lambda.getArguments().getNames();
    Names fused_argument_names;
    fused_argument_names.reserve(left_argument_names.size());

    auto left_body = left_lambda.getExpression();
    for (size_t i = 0; i < left_argument_names.size(); ++i)
    {
        String fresh_name = composedArgumentName(i + 1);
        for (size_t attempt = 1; used_names.contains(fresh_name); ++attempt)
            fresh_name = composedArgumentName(i + 1) + "_" + std::to_string(attempt);
        used_names.insert(fresh_name);

        fused_argument_names.push_back(fresh_name);
        auto fresh_argument = std::make_shared<IdentifierNode>(Identifier(std::move(fresh_name)));
        substituteIdentifier(left_body, left_argument_names[i], fresh_argument);
    }

    /// The composition itself: the body of the left lambda becomes the argument of the right one.
    auto fused_body = right_lambda.getExpression();
    substituteIdentifier(fused_body, right_argument_names[0], left_body);

    auto fused_arguments = std::make_shared<LambdaArgumentsNode>(std::move(fused_argument_names));
    auto fused = std::make_shared<LambdaNode>(std::move(fused_arguments), std::move(fused_body), false /*is_operator*/);
    fused->setAlias(compose_node.getAlias());
    return fused;
}

}
