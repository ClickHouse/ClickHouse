#include <Parsers/ASTDropReplicaQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropReplicaQuery::clone() const
{
    return make_intrusive<ASTDropReplicaQuery>(*this);
}

void ASTDropReplicaQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "DROP REPLICA ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(replica_name);
    formatOnCluster(ostr, s);
}

}
