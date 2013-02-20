#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Запрос SHOW TABLES или SHOW DATABASES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
	bool databases;
	String from;
	String like;

	ASTShowTablesQuery() : databases(false) {}
	ASTShowTablesQuery(StringRange range_) : ASTQueryWithOutput(range_), databases(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "ShowTables"; };

	ASTPtr clone() const
	{
		ASTShowTablesQuery * res = new ASTShowTablesQuery(*this);
		res->children.clear();
		
		if (format)
		{
			res->format = format->clone();
			res->children.push_back(res->format);
		}
		
		return res;
	}
};

}
