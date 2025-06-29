#include <Parsers/ASTFinishCollectingWorkloadQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

void ASTFinishCollectingWorkloadQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "finish collecting workload" << (settings.hilite ? hilite_none : "");
}

}
