#ifndef DBMS_PARSERS_ASTEXPRESSIONLIST_H
#define DBMS_PARSERS_ASTEXPRESSIONLIST_H

#include <DB/Parsers/IAST.h>


namespace DB
{

using Poco::SharedPtr;


/** Список выражений типа "a, b + c, f(d)"
  */
class ASTExpressionList : public IAST
{
public:
	ASTExpressionList() {}
	ASTExpressionList(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "ExpressionList"; }
};

}

#endif
