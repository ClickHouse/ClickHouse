#pragma once

#include <Core/Block_fwd.h>
#include <Core/Field.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>


namespace DB
{

class ExpressionActions;
class IDataType;

using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

using EvaluateConstantExpressionResult = std::pair<Field, std::shared_ptr<const IDataType>>;

/** The value of a constant expression as a single-row column plus its exact type.
  * Unlike `EvaluateConstantExpressionResult`, this keeps the precise SQL type (`UInt8`, `Float32`,
  * `DateTime64`, ...) instead of collapsing it through `Field`'s `NearestFieldType` mapping, and it
  * avoids materializing a `Field` at all. `column` is a size-1 (const) column; read it with the typed
  * `IColumn` accessors (`getUInt`, `getDataAt`, ...) or a `ValueRef`.
  */
using EvaluateConstantExpressionColumnResult = std::pair<ColumnPtr, std::shared_ptr<const IDataType>>;

/** Evaluate constant expression and its type.
  * Used in rare cases - for elements of set for IN, for data to INSERT.
  * Throws exception if it's not a constant expression.
  * Quite suboptimal.
  */
EvaluateConstantExpressionResult evaluateConstantExpression(const ASTPtr & node, const ContextPtr & context);

std::optional<EvaluateConstantExpressionResult> tryEvaluateConstantExpression(const ASTPtr & node, const ContextPtr & context);

/** Same as `evaluateConstantExpression`, but returns the value as a single-row column + exact type,
  * without collapsing the type through `Field`. Prefer this on paths that only need to read a scalar
  * (via `IColumn` typed getters) — it avoids a `Field` materialization and preserves the SQL type.
  */
EvaluateConstantExpressionColumnResult evaluateConstantExpressionAsColumn(const ASTPtr & node, const ContextPtr & context);

std::optional<EvaluateConstantExpressionColumnResult> tryEvaluateConstantExpressionAsColumn(const ASTPtr & node, const ContextPtr & context);

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

using ConstantVariants = std::vector<ColumnsWithTypeAndName>;

/// max_elements is a hint
std::optional<ConstantVariants> evaluateExpressionOverConstantCondition(
    const ActionsDAG::Node * predicate,
    const ActionsDAG::NodeRawConstPtrs & expr,
    const ContextPtr & context,
    size_t max_elements);
}
