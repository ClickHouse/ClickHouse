#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Names.h>

#include <functional>
#include <optional>
#include <string_view>

namespace DB
{

class FunctionNode;
class IdentifierNode;

/** The internal name of the function node that the `f | g` operator parses into. The parser
  * (`ParserExpressionImpl::operators_table`) produces this name for the operator, and nothing
  * else does. It is deliberately not a name a user can write by accident, so the operator does
  * not steal the public name `compose`.
  *
  * The name alone does not reserve anything: `__compose` is a legal identifier too, so what
  * makes a node a composition is the operator syntax the parser recorded in `is_operator`, not
  * the name — see `isFunctionComposition`. A user defined function of either name keeps
  * working when it is called as an ordinary function.
  */
inline constexpr std::string_view function_composition_name = "__compose";

/** Whether the node is the function node the `f | g` operator parses into: a function node with
  * the internal name that the parser marked as written with operator syntax. An ordinary call
  * written as `__compose(x, y)` is not a composition and resolves to the function of that name.
  */
bool isFunctionComposition(const IQueryTreeNode & node);

/** Machinery behind the function composition operator `f | g` and the argument placeholders
  * `_`, `_1`, `_2`, ... Both are pure syntax sugar over lambdas and are resolved entirely at
  * query analysis time by rewriting to ordinary lambdas:
  *
  *   plus(_, 1)                is lifted to   _1 -> plus(_1, 1)
  *   plus(_, 1) | toString(_)  is fused to    __composed_arg_1 -> toString(plus(__composed_arg_1, 1))
  *
  * No runtime support is needed: after the rewrite only ordinary lambdas remain, and the
  * standard lambda machinery (typing, captures, execution) applies unchanged.
  */

/** Collect the names of placeholder identifiers (`_` or `_N`) that occur free in an expression,
  * i.e. are not bound by an enclosing lambda inside the expression. The scan does not descend
  * into subqueries (placeholders are not supported there) and does not descend into nested
  * composition nodes (their placeholders belong to their own operands).
  *
  * A numbered placeholder is `_` followed by a positive integer without leading zeros.
  */
NameSet collectFreePlaceholderNames(const QueryTreeNodePtr & node);

/** Lift a function call with placeholder arguments to an unresolved lambda.
  *
  * Numbered placeholders may appear at any depth and become lambda arguments with the same
  * names, so no substitution is needed; the lambda arity is the largest placeholder number:
  *
  *   plus(_1, 1)          ->  _1 -> plus(_1, 1)
  *   if(_1 > 0, _1, -_1)  ->  _1 -> if(_1 > 0, _1, -_1)
  *   pow(_2, _1)          ->  (_1, _2) -> pow(_2, _1)
  *
  * Anonymous placeholders must be direct arguments of the call and are numbered left to right:
  *
  *   plus(5, _)     ->  _1 -> plus(5, _1)
  *   concat(_, _)   ->  (_1, _2) -> concat(_1, _2)
  *
  * A bare placeholder is the identity function:
  *
  *   _1             ->  _1 -> _1
  *   _              ->  _1 -> _1
  *
  * Mixing anonymous and numbered placeholders in one expression is an error.
  */
QueryTreeNodePtr liftPlaceholdersToLambda(const QueryTreeNodePtr & node);

/** Resolve an identifier operand of a composition to an unresolved lambda, or return nullptr
  * when the name does not denote a function. `required_arity` is set for the right operand
  * (a composition applies its right operand to exactly one value), so a variadic function
  * name like `toString` can be used there; for the left operand the arity must be inferable
  * from the function itself.
  */
using ResolveIdentifierOperand = std::function<QueryTreeNodePtr(const IdentifierNode &, std::optional<size_t> required_arity)>;

/** Fuse a `__compose(f, g)` node (produced by the `f | g` operator) into a single unresolved
  * lambda `(args...) -> g(f(args...))` by substituting the body of `f` for the argument of `g`.
  *
  * Operands are normalized to lambdas first: explicit lambdas are taken as is, expressions
  * with placeholders are lifted with `liftPlaceholdersToLambda`, nested compositions are fused
  * recursively, and identifiers are resolved through `resolve_identifier_operand` (lambda
  * aliases and registered function names).
  *
  * The lambda arguments of `f` are renamed to names that occur in neither operand, so a column
  * referenced from the body of `g` can never be captured by an argument of `f` accidentally.
  */
QueryTreeNodePtr fuseCompositionToLambda(const FunctionNode & compose_node, const ResolveIdentifierOperand & resolve_identifier_operand);

}
