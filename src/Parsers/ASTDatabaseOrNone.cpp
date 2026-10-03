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
    // Always back-quote the database name to avoid collision with the NONE keyword.
    // Using backQuoteIfNeed would emit an unquoted NONE, which the parser would then
    // read back as the sentinel (no default database) instead of the database name.
    ostr << backQuote(database_name);
}

}
