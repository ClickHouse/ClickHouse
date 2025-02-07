#include <Parsers/ASTDropWorkloadQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropWorkloadQuery::clone() const
{
    return std::make_shared<ASTDropWorkloadQuery>(*this);
}

void ASTDropWorkloadQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "DROP WORKLOAD ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << (settings.hilite ? hilite_none : "");
    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(workload_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(ostr, settings);
}

}
