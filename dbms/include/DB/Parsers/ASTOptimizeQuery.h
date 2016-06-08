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

	/// Может быть указана партиция, в которой производить оптимизацию.
	String partition;
	/// Может быть указан флаг - производить оптимизацию "до конца" вместо одного шага.
	bool final;

	ASTOptimizeQuery() = default;
	ASTOptimizeQuery(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "OptimizeQuery_" + database + "_" + table + "_" + partition + "_" + toString(final); };

	ASTPtr clone() const override { return std::make_shared<ASTOptimizeQuery>(*this); }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << "OPTIMIZE TABLE " << (settings.hilite ? hilite_none : "")
			<< (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

		if (!partition.empty())
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " PARTITION " << (settings.hilite ? hilite_none : "")
				<< partition;

		if (final)
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " FINAL" << (settings.hilite ? hilite_none : "");
	}
};

}
