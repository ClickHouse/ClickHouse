#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{

/** Пара из имени и типа. Например, browser FixedString(2).
  */
class ASTNameTypePair : public IAST
{
public:
	/// имя
	String name;
	/// тип
	ASTPtr type;

    ASTNameTypePair() {}
    ASTNameTypePair(StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "NameTypePair_" + name; }

	ASTPtr clone() const
	{
		ASTNameTypePair * res = new ASTNameTypePair(*this);
		res->children.clear();

		if (type) 	{ res->type = type->clone(); 	res->children.push_back(res->type); }

		return res;
	}
};

}

