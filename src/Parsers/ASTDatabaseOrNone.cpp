#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        ostr << "NONE";
        return;
    }
    ostr << backQuoteIfNeed(database_name);
}

}
