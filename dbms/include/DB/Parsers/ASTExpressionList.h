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
	ASTExpressionList() = default;
	ASTExpressionList(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "ExpressionList"; }

	ASTPtr clone() const override
	{
		const auto res = new ASTExpressionList(*this);
		ASTPtr ptr{res};
		res->children.clear();
		
		for (const auto & child : children)
			res->children.emplace_back(child->clone());

		return ptr;
	}
};

}
