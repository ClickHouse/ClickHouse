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
	String getID() const { return "InsertQuery_" + database + "_" + table; };

	ASTPtr clone() const
	{
		ASTInsertQuery * res = new ASTInsertQuery(*this);
		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }

		return res;
	}
};

}
