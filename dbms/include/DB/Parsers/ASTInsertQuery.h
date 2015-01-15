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
	/// Идентификатор запроса INSERT. Используется при репликации.
	String insert_id;
	/// Данные для вставки
	const char * data = nullptr;
	const char * end = nullptr;

	ASTInsertQuery() = default;
	ASTInsertQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "InsertQuery_" + database + "_" + table; };

	ASTPtr clone() const override
	{
		ASTInsertQuery * res = new ASTInsertQuery(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }

		return ptr;
	}
};

}
