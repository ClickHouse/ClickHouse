#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }
    ostr << backQuoteIfNeed(database_name);
}

}
