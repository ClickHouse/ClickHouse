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
	StringRange range;
	ASTs children;

	ASTExpressionList() {}
	ASTExpressionList(StringRange range_) : range(range_) {}
	
	/** Получить кусок текста, откуда был получен этот элемент. */
	StringRange getRange() { return range; }
};

}

#endif
