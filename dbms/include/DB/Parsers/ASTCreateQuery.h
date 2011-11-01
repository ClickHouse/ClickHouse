#pragma once

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{


/** CREATE TABLE или ATTACH TABLE запрос
  */
class ASTCreateQuery : public IAST
{
public:
	bool attach;	/// Запрос ATTACH TABLE, а не CREATE TABLE.
	bool if_not_exists;
	String database;
	String table;
	ASTPtr columns;
	ASTPtr storage;
	String as_database;
	String as_table;
	ASTPtr select;

	ASTCreateQuery() {}
	ASTCreateQuery(StringRange range_) : IAST(range_), attach(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };
};

}
