#pragma once

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
	String getID() const { return "ExpressionList"; }

	ASTPtr clone() const
	{
		ASTExpressionList * res = new ASTExpressionList(*this);
		res->children.clear();
		
		for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
			res->children.push_back((*it)->clone());

		return res;
	}
};

}
