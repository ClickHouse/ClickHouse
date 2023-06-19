#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST.h>

#include <memory>
#include <optional>


namespace DB
{

class ExpressionActions;
class IDataType;

using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Evaluate constant expression and its type.
  * Used in rare cases - for elements of set for IN, for data to INSERT.
  * Throws exception if it's not a constant expression.
  * Quite suboptimal.
  */
std::pair<Field, std::shared_ptr<const IDataType>> evaluateConstantExpression(const ASTPtr & node, const ContextPtr & context);


/** Evaluate constant expression and returns ASTLiteral with its value.
  */
ASTPtr evaluateConstantExpressionAsLiteral(const ASTPtr & node, const ContextPtr & context);


/** Evaluate constant expression and returns ASTLiteral with its value.
  * Also, if AST is identifier, then return string literal with its name.
  * Useful in places where some name may be specified as identifier, or as result of a constant expression.
  */
ASTPtr evaluateConstantExpressionOrIdentifierAsLiteral(const ASTPtr & node, const ContextPtr & context);

/** The same as evaluateConstantExpressionOrIdentifierAsLiteral(...),
 *  but if result is an empty string, replace it with current database name
 *  or default database name.
 */
ASTPtr evaluateConstantExpressionForDatabaseName(const ASTPtr & node, const ContextPtr & context);

/** Try to fold condition to countable set of constant values.
  * @param node a condition that we try to fold.
  * @param target_expr expression evaluated over a set of constants.
  * @param limit limit for number of values
  * @return optional blocks each with a single row and a single column for target expression,
  *         or empty blocks if condition is always false,
  *         or nothing if condition can't be folded to a set of constants.
  */
std::optional<Blocks> evaluateExpressionOverConstantCondition(const ASTPtr & node, const ExpressionActionsPtr & target_expr, size_t & limit);

}
