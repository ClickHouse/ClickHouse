#include <Parsers/ASTCreateShardQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ASTPtr ASTCreateShardQuery::clone() const
{
    return make_intrusive<ASTCreateShardQuery>(*this);
}

void ASTCreateShardQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE SHARD ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(shard_name) << " (";
    for (size_t i = 0; i < replicas.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << backQuoteIfNeed(replicas[i]);
    }
    ostr << ") SETTINGS "
         << "weight = " << weight << ", internal_replication = " << (internal_replication ? "true" : "false");
    formatOnCluster(ostr, s);
}

}
