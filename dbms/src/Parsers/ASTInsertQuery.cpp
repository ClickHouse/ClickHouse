#include <iomanip>
#include <Parsers/ASTInsertQuery.h>
#include <Common/config.h>


namespace DB
{

void ASTInsertQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO ";
    if (table_function)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FUNCTION ";
        table_function->formatImpl(settings, state, frame);
    }
    else
        settings.ostr << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

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
#if ENABLE_INSERT_INFILE
            if (in_file)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " INFILE " << (settings.hilite ? hilite_none : "");
                in_file->formatImpl(settings, state, frame);
            }
#endif
        }
        else
        {
#if ENABLE_INSERT_INFILE
            if (in_file)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " INFILE " << (settings.hilite ? hilite_none : "");
                in_file->formatImpl(settings, state, frame);
            }
            else
#endif
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
            }
        }
    }
}

}
