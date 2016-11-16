#pragma once

#include <memory>
#include <DB/Core/Field.h>


namespace DB
{

class IAST;
class Context;

/** Evaluate constant expression.
  * Used in rare cases - for elements of set for IN, for data to INSERT.
  * Quite suboptimal.
  */
Field evaluateConstantExpression(std::shared_ptr<IAST> & node, const Context & context);


/** Evaluate constant expression
  *  and returns ASTLiteral with its value.
  */
std::shared_ptr<IAST> evaluateConstantExpressionAsLiteral(std::shared_ptr<IAST> & node, const Context & context);


/** Evaluate constant expression
  *  and returns ASTLiteral with its value.
  * Also, if AST is identifier, then return string literal with its name.
  * Useful in places where some name may be specified as identifier, or as result of a constant expression.
  */
std::shared_ptr<IAST> evaluateConstantExpressionOrIdentidierAsLiteral(std::shared_ptr<IAST> & node, const Context & context);

}
