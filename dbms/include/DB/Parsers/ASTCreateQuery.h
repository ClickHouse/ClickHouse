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
	bool is_view;
	bool is_materialized_view;
	bool is_populate;
	bool is_temporary;
	String database;
	String table;
	ASTPtr columns;
	ASTPtr storage;
	ASTPtr inner_storage;	/// Внутренний engine для запроса CREATE MATERIALIZED VIEW
	String as_database;
	String as_table;
	ASTPtr select;

	ASTCreateQuery() : attach(false), if_not_exists(false), is_view(false), is_materialized_view(false), is_populate(false), is_temporary(false) {}
	ASTCreateQuery(StringRange range_) : IAST(range_), attach(false), if_not_exists(false), is_view(false), is_materialized_view(false), is_populate(false), is_temporary(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };

	ASTPtr clone() const
	{
		ASTCreateQuery * res = new ASTCreateQuery(*this);
		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (storage) 	{ res->storage = storage->clone(); 	res->children.push_back(res->storage); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }
		if (inner_storage) 	{ res->inner_storage = inner_storage->clone(); 	res->children.push_back(res->inner_storage); }

		return res;
	}
};

}
