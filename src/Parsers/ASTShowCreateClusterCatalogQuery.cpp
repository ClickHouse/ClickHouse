#include <Parsers/ASTShowCreateClusterCatalogQuery.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ASTPtr ASTShowCreateClusterCatalogQuery::clone() const
{
    auto res = make_intrusive<ASTShowCreateClusterCatalogQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowCreateClusterCatalogQuery::formatQueryImpl(WriteBuffer & ostr, const IAST::FormatSettings &, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << (kind == Kind::Cluster ? "SHOW CREATE CLUSTER " : "SHOW CREATE SHARD ") << backQuoteIfNeed(name);
}

}
