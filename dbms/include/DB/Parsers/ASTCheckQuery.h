#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{

struct ASTCheckQuery : public IAST
{
	ASTCheckQuery(StringRange range_ = StringRange()) : IAST(range_) {};
	ASTCheckQuery(const ASTCheckQuery & ast) = default;

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return ("CheckQuery_" + database + "_" + table); };

	ASTPtr clone() const
	{
		return new ASTCheckQuery(*this);
	}

	std::string database;
	std::string table;
};

}
