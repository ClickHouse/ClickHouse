#pragma once

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{


/** INSERT запрос
  */
class ASTInsertQuery : public IAST
{
public:
	String database;
	String table;
	ASTPtr columns;
	String format;
	ASTPtr select;
	/// Данные для вставки
	const char * data;
	const char * end;

	ASTInsertQuery() {}
	ASTInsertQuery(StringRange range_) : IAST(range_), data(NULL), end(NULL) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "InsertQuery_" + database + "_" + table; };
};

}
