#include <mysqlxx/Manip.h>
#include <DB/Parsers/ASTInsertQuery.h>


namespace DB
{

void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	frame.need_parens = false;

	settings.ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO " << (settings.hilite ? hilite_none : "")
	<< (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

	if (!insert_id.empty())
		settings.ostr << (settings.hilite ? hilite_keyword : "") << " ID = " << (settings.hilite ? hilite_none : "")
		<< mysqlxx::quote << insert_id;

	if (columns)
	{
		settings.ostr << " (";
		columns->formatImpl(settings, state, frame);
		settings.ostr << ")";
	}

	if (select)
	{
		settings.ostr << " ";
		select->formatImpl(settings, state, frame);
	}
	else
	{
		if (!format.empty())
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " FORMAT " << (settings.hilite ? hilite_none : "") << format;
		}
		else
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
		}
	}
}

}
