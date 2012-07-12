#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** USE запрос
  */
class ASTUseQuery : public IAST
{
public:
	String database;

	ASTUseQuery() {}
	ASTUseQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "UseQuery_" + database; };

	ASTPtr clone() const { return new ASTUseQuery(*this); }
};

}
