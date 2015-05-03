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
	bool attach{false};	/// Запрос ATTACH TABLE, а не CREATE TABLE.
	bool if_not_exists{false};
	bool is_view{false};
	bool is_materialized_view{false};
	bool is_populate{false};
	bool is_temporary{false};
	String database;
	String table;
	ASTPtr columns;
	ASTPtr storage;
	ASTPtr inner_storage;	/// Внутренний engine для запроса CREATE MATERIALIZED VIEW
	String as_database;
	String as_table;
	ASTPtr select;

	ASTCreateQuery() = default;
	ASTCreateQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };

	ASTPtr clone() const override
	{
		ASTCreateQuery * res = new ASTCreateQuery(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (storage) 	{ res->storage = storage->clone(); 	res->children.push_back(res->storage); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }
		if (inner_storage) 	{ res->inner_storage = inner_storage->clone(); 	res->children.push_back(res->inner_storage); }

		return ptr;
	}
};

}
