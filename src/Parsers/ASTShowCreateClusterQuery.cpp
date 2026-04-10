#include <Parsers/ASTShowCreateClusterQuery.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ASTPtr ASTShowCreateClusterQuery::clone() const
{
    auto res = make_intrusive<ASTShowCreateClusterQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowCreateClusterQuery::formatQueryImpl(WriteBuffer & ostr, const IAST::FormatSettings &, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "SHOW CREATE CLUSTER " << backQuoteIfNeed(cluster_name);
}

}
