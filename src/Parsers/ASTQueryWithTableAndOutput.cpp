#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTQueryWithTableAndOutput::formatHelper(const FormatSettings & settings, const char * name) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
}

}

