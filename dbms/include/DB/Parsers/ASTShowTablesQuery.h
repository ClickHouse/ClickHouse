#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** Запрос SHOW TABLES или SHOW DATABASES
  */
class ASTShowTablesQuery : public IAST
{
public:
	bool databases;
	String from;
	String like;

	ASTShowTablesQuery() : databases(false) {}
	ASTShowTablesQuery(StringRange range_) : IAST(range_), databases(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "ShowTables"; };

	ASTPtr clone() const { return new ASTShowTablesQuery(*this); }
};

}
