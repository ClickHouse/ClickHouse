#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        settings.writeKeyword("NONE");
        return;
    }
    settings.ostr << backQuoteIfNeed(database_name);
}

}
