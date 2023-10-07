#include <IO/Operators.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Common/SipHash.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (streaming)
        format.ostr << (format.hilite ? hilite_keyword : "") << "STREAM " << (format.hilite ? hilite_none : "");

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
    hash_state.update(streaming);

    if (periodic_interval)
        periodic_interval->updateTreeHashImpl(hash_state);

    IAST::updateTreeHashImpl(hash_state);
}

}
