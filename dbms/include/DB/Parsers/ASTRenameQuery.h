#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** RENAME запрос
  */
class ASTRenameQuery : public IAST
{
public:
	struct Table
	{
		String database;
		String table;
	};

	struct Element
	{
		Table from;
		Table to;
	};

	typedef std::vector<Element> Elements;
	Elements elements;

	ASTRenameQuery() {}
	ASTRenameQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Rename"; };

	ASTPtr clone() const { return new ASTRenameQuery(*this); }
};

}
