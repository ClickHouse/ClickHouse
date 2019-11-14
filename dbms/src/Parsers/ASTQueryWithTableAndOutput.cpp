#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

void ASTQueryWithTableAndOutput::formatHelper(const FormatSettings & settings, const char * name) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << (database ? backQuoteIfNeed(getIdentifierName(database)) + "." : "") << backQuoteIfNeed(getIdentifierName(table));
}

String ASTQueryWithTableAndOutput::getTableAndDatabaseID(char delim) const
{
    return database ? getIdentifierName(database) + delim + getIdentifierName(table) : getIdentifierName(table);
}

void ASTQueryWithTableAndOutput::formatTableAndDatabase(const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked frame) const
{
    if (onlyDatabase())
        settings.ostr << backQuoteIfNeed(getIdentifierName(database));
    else
    {
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

        if (table)
        {
            if (database)
            {
                settings.ostr << indent_str << backQuoteIfNeed(getIdentifierName(database));
                settings.ostr << ".";
            }
            settings.ostr << indent_str << backQuoteIfNeed(getIdentifierName(table));
        }
    }
}

}

