#include <Parsers/ASTSQLClusterQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/FieldVisitorToSetting.h>
#include <Parsers/formatSettingName.h>
#include <IO/Operators.h>
#include <Common/quoteString.h>
#include <base/find_symbols.h>

#include <cstring>
#include <string_view>
#include <vector>


namespace DB
{

namespace
{

bool equalsKeywordIgnoreCase(std::string_view name, std::string_view keyword)
{
    return name.size() == keyword.size()
        && 0 == strncasecmp(name.data(), keyword.data(), name.size()); // NOLINT(bugprone-suspicious-stringview-data-usage)
}

void formatSQLClusterSettingName(const String & name, WriteBuffer & ostr)
{
    /// Unquoted, `shard` / `replica` (and `shard.x` / `replica.x`) are parsed as the
    /// `SHARD` / `REPLICA` keywords rather than as a property assignment.
    std::vector<std::string_view> parts;
    splitInto<'.'>(parts, name);
    if (!parts.empty() && (equalsKeywordIgnoreCase(parts.front(), "shard") || equalsKeywordIgnoreCase(parts.front(), "replica")))
        ostr << backQuote(name);
    else
        formatSettingName(name, ostr);
}

void formatPropertiesAssignments(
    WriteBuffer & ostr,
    const SettingsChanges & properties,
    const IAST::FormatSettings & settings)
{
    for (size_t i = 0; i < properties.size(); ++i)
    {
        if (i != 0)
            ostr << ", ";

        formatSQLClusterSettingName(properties[i].name, ostr);
        if (settings.show_secrets || (properties[i].name != "password" && properties[i].name != "secret"))
            ostr << " = " << applyVisitor(FieldVisitorToSetting(), properties[i].value);
        else
            ostr << " = '[HIDDEN]'";
    }
}

}

ASTPtr ASTSQLClusterReplica::clone() const
{
    auto res = make_intrusive<ASTSQLClusterReplica>(*this);
    return res;
}

void ASTSQLClusterReplica::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    formatPropertiesAssignments(ostr, properties, settings);
}

ASTPtr ASTSQLClusterShard::clone() const
{
    auto res = make_intrusive<ASTSQLClusterShard>(*this);
    res->replicas.clear();
    res->replicas.reserve(replicas.size());
    for (const auto & replica : replicas)
        res->replicas.push_back(replica->clone());
    return res;
}

void ASTSQLClusterShard::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "SHARD (";
    bool need_comma = false;

    if (!properties.empty())
    {
        formatPropertiesAssignments(ostr, properties, settings);
        need_comma = true;
    }

    for (const auto & replica : replicas)
    {
        if (need_comma)
            ostr << ", ";
        ostr << "REPLICA (";
        replica->format(ostr, settings, state, frame);
        ostr << ")";
        need_comma = true;
    }

    ostr << ")";
}

ASTPtr ASTSQLClusterDefinition::clone() const
{
    auto res = make_intrusive<ASTSQLClusterDefinition>(*this);
    res->shards.clear();
    res->shards.reserve(shards.size());
    for (const auto & shard : shards)
        res->shards.push_back(shard->clone());
    return res;
}

void ASTSQLClusterDefinition::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "(";
    bool need_comma = false;

    if (!cluster_properties.empty())
    {
        formatPropertiesAssignments(ostr, cluster_properties, settings);
        need_comma = true;
    }

    for (const auto & shard : shards)
    {
        if (need_comma)
            ostr << ", ";
        shard->format(ostr, settings, state, frame);
        need_comma = true;
    }

    ostr << ")";
}

ASTPtr ASTCreateSQLClusterQuery::clone() const
{
    auto res = make_intrusive<ASTCreateSQLClusterQuery>(*this);
    if (definition)
        res->definition = definition->clone();
    return res;
}

void ASTCreateSQLClusterQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "CREATE CLUSTER ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(cluster_name) << " ";
    formatOnCluster(ostr, settings);
    definition->format(ostr, settings, state, frame);
}

ASTPtr ASTAlterSQLClusterQuery::clone() const
{
    auto res = make_intrusive<ASTAlterSQLClusterQuery>(*this);
    if (definition)
        res->definition = definition->clone();
    return res;
}

void ASTAlterSQLClusterQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "ALTER CLUSTER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(cluster_name) << " ";
    formatOnCluster(ostr, settings);
    definition->format(ostr, settings, state, frame);
}

ASTPtr ASTDropSQLClusterQuery::clone() const
{
    return make_intrusive<ASTDropSQLClusterQuery>(*this);
}

void ASTDropSQLClusterQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << "DROP CLUSTER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(cluster_name);
    formatOnCluster(ostr, settings);
}

}
