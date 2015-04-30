#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** OPTIMIZE запрос
  */
class ASTOptimizeQuery : public IAST
{
public:
	String database;
	String table;

	ASTOptimizeQuery() = default;
	ASTOptimizeQuery(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "OptimizeQuery_" + database + "_" + table; };

	void updateHashWith(SipHash & hash) const override
	{
		hash.update("OptimizeQuery", strlen("OptimizeQuery") + 1);
		hash.update(database.data(), database.size() + 1);
		hash.update(table.data(), table.size() + 1);
	}

	ASTPtr clone() const override { return new ASTOptimizeQuery(*this); }
};

}
