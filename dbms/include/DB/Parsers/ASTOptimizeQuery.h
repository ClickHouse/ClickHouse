#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** OPTIMIZE запрос
  */
class ASTOptimizeQuery : public IAST
{
public:
	String database;
	String table;

	ASTOptimizeQuery() {}
	ASTOptimizeQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "OptimizeQuery_" + database + "_" + table; };

	ASTPtr clone() const { return new ASTOptimizeQuery(*this); }
};

}
