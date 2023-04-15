#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(FormattingBuffer out) const
{
    if (none)
    {
        out.writeKeyword("NONE");
        return;
    }
    out.ostr << backQuoteIfNeed(database_name);
}

}
