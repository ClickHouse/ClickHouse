#pragma once

#include <DB/Parsers/ASTQueryWithOutput.h>

namespace DB
{

struct ASTCheckQuery : public ASTQueryWithOutput
{
	ASTCheckQuery(StringRange range_ = StringRange()) : ASTQueryWithOutput(range_) {};

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return ("CheckQuery_" + database + "_" + table); };

	ASTPtr clone() const override
	{
		return std::make_shared<ASTCheckQuery>(*this);
	}

	std::string database;
	std::string table;

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		std::string nl_or_nothing = settings.one_line ? "" : "\n";

		std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
		std::string nl_or_ws = settings.one_line ? " " : "\n";

		settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK TABLE " << (settings.hilite ? hilite_none : "");

		if (!table.empty())
		{
			if (!database.empty())
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << database << (settings.hilite ? hilite_none : "");
				settings.ostr << ".";
			}
			settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << table << (settings.hilite ? hilite_none : "");
		}
		settings.ostr << nl_or_ws;
	}
};

}
