#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** EXISTS запрос
  */
class ASTExistsQuery : public IAST
{
public:
	String database;
	String table;

	ASTExistsQuery() {}
	ASTExistsQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "ExistsQuery_" + database + "_" + table; };

	ASTPtr clone() const { return new ASTExistsQuery(*this); }
};

}
