#include <Parsers/ASTCreateClusterCatalogQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTCreateClusterCatalogQuery::clone() const
{
    return make_intrusive<ASTCreateClusterCatalogQuery>(*this);
}

namespace
{

void formatPropertiesList(WriteBuffer & ostr, const SettingsChanges & properties, bool show_secrets)
{
    ostr << "PROPERTIES (";
    for (size_t i = 0; i < properties.size(); ++i)
    {
        if (i)
            ostr << ", ";
        const auto & ch = properties[i];
        formatSettingName(ch.name, ostr);
        if (show_secrets)
            ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
        else
            ostr << " = '[HIDDEN]'";
    }
    ostr << ")";
}

}

void ASTCreateClusterCatalogQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & s, IAST::FormatState &, IAST::FormatStateStacked) const
{
    const bool is_cluster = kind == Kind::Cluster;

    ostr << (is_cluster ? "CREATE CLUSTER " : "CREATE SHARD ");
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(name);

    /// Member list: `CREATE CLUSTER name (m1, m2, ...)` vs `CREATE SHARD name REPLICA (r1, r2, ...)`.
    ostr << (is_cluster ? " (" : " REPLICA (");
    for (size_t i = 0; i < members.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << backQuoteIfNeed(members[i]);
    }
    ostr << ")";

    if (is_cluster)
    {
        /// Cluster-level `PROPERTIES` is optional — emit only when present.
        if (!properties.empty())
        {
            ostr << " ";
            formatPropertiesList(ostr, properties, s.show_secrets);
        }
    }
    else
    {
        /// Shard-level `PROPERTIES` is always rendered (`SHOW CREATE SHARD` must show `weight` /
        /// `internal_replication`; defaults are inlined when the list is empty).
        ostr << " ";
        if (properties.empty())
            ostr << "PROPERTIES (weight = 1, internal_replication = false)";
        else
            formatPropertiesList(ostr, properties, s.show_secrets);
    }

    formatOnCluster(ostr, s);
    if (!cluster.empty() && sync)
        ostr << " SYNC";
}

}
