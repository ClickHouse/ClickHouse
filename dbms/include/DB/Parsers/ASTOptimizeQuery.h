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

	ASTPtr clone() const override { return new ASTOptimizeQuery(*this); }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "")
			<< (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
	}
};

}
