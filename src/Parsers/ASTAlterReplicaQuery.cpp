#include <Parsers/ASTAlterReplicaQuery.h>

#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/formatSettingName.h>

namespace DB
{

ASTPtr ASTAlterReplicaQuery::clone() const
{
    return make_intrusive<ASTAlterReplicaQuery>(*this);
}

void ASTAlterReplicaQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "ALTER REPLICA " << backQuoteIfNeed(replica_name) << " MODIFY PROPERTIES (";
    for (size_t i = 0; i < properties.size(); ++i)
    {
        if (i)
            ostr << ", ";
        const auto & ch = properties[i];
        formatSettingName(ch.name, ostr);
        if (s.show_secrets)
            ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
        else
            ostr << " = '[HIDDEN]'";
    }
    ostr << ")";
    formatOnCluster(ostr, s);
}

}
