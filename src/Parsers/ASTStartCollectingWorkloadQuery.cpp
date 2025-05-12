#include <Parsers/ASTStartCollectingWorkloadQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTStartCollectingWorkloadQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "start collecting workload" << (settings.hilite ? hilite_none : "");
}

}
