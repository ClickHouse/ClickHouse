#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** SELECT запрос
  */
class ASTSelectQuery : public IAST
{
public:
	ASTPtr select_expression_list;
	ASTPtr database;
	ASTPtr table;
	ASTPtr where_expression;
	ASTPtr group_expression_list;
	ASTPtr having_expression;
	ASTPtr order_expression_list;
	ASTPtr limit_offset;
	ASTPtr limit_length;
	ASTPtr format;
	
	ASTSelectQuery() {}
	ASTSelectQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "SelectQuery"; };
};

}
