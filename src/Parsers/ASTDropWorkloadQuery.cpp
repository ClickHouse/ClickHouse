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
    ostr << "DROP WORKLOAD ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(workload_name);
    formatOnCluster(ostr, settings);
}

}
