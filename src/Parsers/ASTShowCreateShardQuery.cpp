#include <Parsers/ASTShowCreateShardQuery.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ASTPtr ASTShowCreateShardQuery::clone() const
{
    auto res = make_intrusive<ASTShowCreateShardQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowCreateShardQuery::formatQueryImpl(WriteBuffer & ostr, const IAST::FormatSettings &, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "SHOW CREATE SHARD " << backQuoteIfNeed(shard_name);
}

}
