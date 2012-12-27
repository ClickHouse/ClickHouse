#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

using Poco::SharedPtr;


/** Элемент выражения, после которого стоит ASC или DESC
  */
class ASTOrderByElement : public IAST
{
public:
	int direction;	/// 1, если ASC, -1, если DESC
	
	ASTOrderByElement() {}
	ASTOrderByElement(StringRange range_, int direction_) : IAST(range_), direction(direction_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "OrderByElement"; }

	ASTPtr clone() const { return new ASTOrderByElement(*this); }
};

}
