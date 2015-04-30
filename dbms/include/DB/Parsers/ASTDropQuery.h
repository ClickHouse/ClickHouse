#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{


/** DROP запрос
  */
class ASTDropQuery : public IAST
{
public:
	bool detach{false};	/// Запрос DETACH, а не DROP.
	bool if_exists{false};
	String database;
	String table;

	ASTDropQuery() = default;
	ASTDropQuery(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return (detach ? "DetachQuery_" : "DropQuery_") + database + "_" + table; };

	void updateHashWith(SipHash & hash) const override
	{
		hash.update(reinterpret_cast<const char *>(&detach), sizeof(detach));
		hash.update("DropQuery", strlen("DropQuery") + 1);
		hash.update(database.data(), database.size() + 1);
		hash.update(table.data(), table.size() + 1);
	}

	ASTPtr clone() const override { return new ASTDropQuery(*this); }
};

}
