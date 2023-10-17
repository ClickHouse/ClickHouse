#include <IO/Operators.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Common/SipHash.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    int elems = 0;

    if (periodic_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "PERIODIC " << (format.hilite ? hilite_none : "");
        periodic_interval->format(format);
        ++elems;
    }
}

void ASTEmitQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    if (periodic_interval)
        periodic_interval->updateTreeHashImpl(hash_state);

    IAST::updateTreeHashImpl(hash_state);
}

}
