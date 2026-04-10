#include <Parsers/ASTDropClusterQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropClusterQuery::clone() const
{
    return make_intrusive<ASTDropClusterQuery>(*this);
}

void ASTDropClusterQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP CLUSTER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(cluster_name);
    formatOnCluster(ostr, s);
}

}
