#include <Parsers/ASTDropShardQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropShardQuery::clone() const
{
    return make_intrusive<ASTDropShardQuery>(*this);
}

void ASTDropShardQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP SHARD ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(shard_name);
    formatOnCluster(ostr, s);
}

}
