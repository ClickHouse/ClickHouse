#include <Parsers/ASTDropClusterCatalogQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropClusterCatalogQuery::clone() const
{
    return make_intrusive<ASTDropClusterCatalogQuery>(*this);
}

void ASTDropClusterCatalogQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << (kind == Kind::Cluster ? "DROP CLUSTER " : "DROP SHARD ");
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(name);
    formatOnCluster(ostr, s);
}

}
