#include <Parsers/ASTDropWorkloadQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropWorkloadQuery::clone() const
{
    return std::make_shared<ASTDropWorkloadQuery>(*this);
}

void ASTDropWorkloadQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP WORKLOAD ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(workload_name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
