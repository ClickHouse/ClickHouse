#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** EXISTS запрос
  */
class ASTExistsQuery : public ASTQueryWithOutput
{
public:
	String database;
	String table;

	ASTExistsQuery() {}
	ASTExistsQuery(StringRange range_) : ASTQueryWithOutput(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "ExistsQuery_" + database + "_" + table; };

	ASTPtr clone() const
	{
		ASTExistsQuery * res = new ASTExistsQuery(*this);
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
