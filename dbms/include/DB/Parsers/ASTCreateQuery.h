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
	ASTCreateQuery(StringRange range_) : IAST(range_), attach(false), if_not_exists(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };

	ASTPtr clone() const
	{
		ASTCreateQuery * res = new ASTCreateQuery(*this);
		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (storage) 	{ res->storage = storage->clone(); 	res->children.push_back(res->storage); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }

		return res;
	}
};

}
