#include <Parsers/ASTDatabaseOrNone.h>
#include <IO/Operators.h>

namespace DB
{
void ASTDatabaseOrNone::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }
    settings.ostr << database_name;
}

}
