#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** USE запрос
  */
class ASTUseQuery : public IAST
{
public:
	String database;

	ASTUseQuery() = default;
	ASTUseQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "UseQuery_" + database; };

	ASTPtr clone() const override { return new ASTUseQuery(*this); }
};

}
