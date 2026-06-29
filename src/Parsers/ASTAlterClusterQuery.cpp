#include <Parsers/ASTAlterClusterQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTAlterClusterQuery::clone() const
{
    return make_intrusive<ASTAlterClusterQuery>(*this);
}

void ASTAlterClusterQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "ALTER CLUSTER ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(cluster_name);

    switch (command)
    {
        case AlterClusterCommand::AddShard:
        {
            ostr << " ADD SHARD ";
            for (size_t i = 0; i < add_shard_members.size(); ++i)
            {
                if (i)
                    ostr << ", ";
                ostr << backQuoteIfNeed(add_shard_members[i]);
            }
            break;
        }
        case AlterClusterCommand::DropShard:
        {
            ostr << " DROP SHARD ";
            for (size_t i = 0; i < drop_shard_members.size(); ++i)
            {
                if (i)
                    ostr << ", ";
                ostr << backQuoteIfNeed(drop_shard_members[i]);
            }
            break;
        }
        case AlterClusterCommand::ReplaceClusterMembers:
        {
            auto format_list = [&](const std::vector<String> & ids)
            {
                if (ids.size() == 1)
                    ostr << backQuoteIfNeed(ids[0]);
                else
                {
                    ostr << "(";
                    for (size_t i = 0; i < ids.size(); ++i)
                    {
                        if (i)
                            ostr << ", ";
                        ostr << backQuoteIfNeed(ids[i]);
                    }
                    ostr << ")";
                }
            };
            for (size_t c = 0; c < member_replace_clauses.size(); ++c)
            {
                if (c)
                    ostr << ", REPLACE ";
                else
                    ostr << " REPLACE ";
                const auto & cl = member_replace_clauses[c];
                format_list(cl.from_members);
                ostr << " TO ";
                format_list(cl.to_members);
            }
            if (!cluster_definition_properties.empty())
            {
                ostr << " MODIFY PROPERTIES (";
                for (size_t i = 0; i < cluster_definition_properties.size(); ++i)
                {
                    if (i)
                        ostr << ", ";
                    const auto & ch = cluster_definition_properties[i];
                    formatSettingName(ch.name, ostr);
                    if (s.show_secrets)
                        ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
                    else
                        ostr << " = '[HIDDEN]'";
                }
                ostr << ")";
            }
            break;
        }
        case AlterClusterCommand::ModifyShard:
            ostr << " MODIFY SHARD " << backQuoteIfNeed(modify_shard_name) << " ";
            if (modify_shard_properties.empty())
            {
                ostr << "PROPERTIES (weight = 1, internal_replication = false)";
            }
            else
            {
                ostr << "PROPERTIES (";
                for (size_t i = 0; i < modify_shard_properties.size(); ++i)
                {
                    if (i)
                        ostr << ", ";
                    const auto & ch = modify_shard_properties[i];
                    formatSettingName(ch.name, ostr);
                    if (s.show_secrets)
                        ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
                    else
                        ostr << " = '[HIDDEN]'";
                }
                ostr << ")";
            }
            break;
        case AlterClusterCommand::RenameShard:
            ostr << " RENAME SHARD " << backQuoteIfNeed(rename_shard_from) << " TO " << backQuoteIfNeed(rename_shard_to);
            break;
    }

    formatOnCluster(ostr, s);
}

}
