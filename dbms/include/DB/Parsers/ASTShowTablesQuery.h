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
	bool databases{false};
	String from;
	String like;
	bool not_like{false};

	ASTShowTablesQuery() = default;
	ASTShowTablesQuery(const StringRange range_) : ASTQueryWithOutput(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "ShowTables"; };

	ASTPtr clone() const override
	{
		ASTShowTablesQuery * res = new ASTShowTablesQuery(*this);
		ASTPtr ptr{res};

		res->children.clear();
		
		if (format)
		{
			res->format = format->clone();
			res->children.push_back(res->format);
		}
		
		return ptr;
	}
};

}
