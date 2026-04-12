#include <Parsers/ASTCreateClusterQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
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
    if (!cluster_properties.empty())
    {
        ostr << " PROPERTIES (";
        for (size_t i = 0; i < cluster_properties.size(); ++i)
        {
            if (i)
                ostr << ", ";
            const auto & ch = cluster_properties[i];
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
