#pragma once

#include <mysqlxx/Manip.h>
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

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		if (databases)
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DATABASES" << (settings.hilite ? hilite_none : "");
		}
		else
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW TABLES" << (settings.hilite ? hilite_none : "");

			if (!from.empty())
				settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
					<< backQuoteIfNeed(from);

			if (!like.empty())
				settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIKE " << (settings.hilite ? hilite_none : "")
					<< mysqlxx::quote << like;
		}

		if (format)
		{
			std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
			settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str << "FORMAT " << (settings.hilite ? hilite_none : "");
			format->formatImpl(settings, state, frame);
		}
	}
};

}
