#pragma once

#include <memory>
#include <Core/Field.h>


namespace DB
{

class IAST;
class Context;
class IDataType;

/** Evaluate constant expression and its type.
  * Used in rare cases - for elements of set for IN, for data to INSERT.
  * Quite suboptimal.
  */
std::pair<Field, std::shared_ptr<IDataType>> evaluateConstantExpression(std::shared_ptr<IAST> & node, const Context & context);


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
