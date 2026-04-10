#include <Parsers/ASTCreateClusterQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateClusterQuery::clone() const
{
    return make_intrusive<ASTCreateClusterQuery>(*this);
}

void ASTCreateClusterQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE CLUSTER ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(cluster_name) << " (";
    for (size_t i = 0; i < members.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << backQuoteIfNeed(members[i]);
    }
    ostr << ")";
    formatOnCluster(ostr, s);
}

}
