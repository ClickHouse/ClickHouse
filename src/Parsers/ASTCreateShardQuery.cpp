#include <Parsers/ASTCreateShardQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateShardQuery::clone() const
{
    return make_intrusive<ASTCreateShardQuery>(*this);
}

void ASTCreateShardQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "CREATE SHARD ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(shard_name) << " REPLICA (";
    for (size_t i = 0; i < replicas.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << backQuoteIfNeed(replicas[i]);
    }
    ostr << ") ";
    if (shard_properties.empty())
    {
        ostr << "PROPERTIES (weight = 1, internal_replication = false)";
    }
    else
    {
        ostr << "PROPERTIES (";
        for (size_t i = 0; i < shard_properties.size(); ++i)
        {
            if (i)
                ostr << ", ";
            const auto & ch = shard_properties[i];
            formatSettingName(ch.name, ostr);
            if (s.show_secrets)
                ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
            else
                ostr << " = '[HIDDEN]'";
        }
        ostr << ")";
    }
    formatOnCluster(ostr, s);
    if (!cluster.empty() && sync)
        ostr << " SYNC";
}

}
