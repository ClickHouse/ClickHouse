#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** DROP запрос
  */
class ASTDropQuery : public IAST
{
public:
	bool detach;	/// Запрос DETACH, а не DROP.
	bool if_exists;
	String database;
	String table;

	ASTDropQuery() : detach(false), if_exists(false) {}
	ASTDropQuery(StringRange range_) : IAST(range_), detach(false), if_exists(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return (detach ? "DetachQuery_" : "DropQuery_") + database + "_" + table; };

	ASTPtr clone() const { return new ASTDropQuery(*this); }
};

}
