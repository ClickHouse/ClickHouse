#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** DROP запрос
  */
class ASTDropQuery : public IAST
{
public:
	bool detach{false};	/// Запрос DETACH, а не DROP.
	bool if_exists{false};
	String database;
	String table;

	ASTDropQuery() = default;
	ASTDropQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return (detach ? "DetachQuery_" : "DropQuery_") + database + "_" + table; };

	ASTPtr clone() const override { return new ASTDropQuery(*this); }
};

}
