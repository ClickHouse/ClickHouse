#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{

struct ASTCheckQuery : public IAST
{
	ASTCheckQuery(StringRange range_ = StringRange()) : IAST(range_) {};

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return ("CheckQuery_" + database + "_" + table); };

	ASTPtr clone() const override
	{
		return new ASTCheckQuery(*this);
	}

	std::string database;
	std::string table;
};

}
