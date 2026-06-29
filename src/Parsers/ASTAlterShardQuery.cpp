#include <Parsers/ASTAlterShardQuery.h>
#include <Parsers/formatSettingName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTAlterShardQuery::clone() const
{
    return make_intrusive<ASTAlterShardQuery>(*this);
}

void ASTAlterShardQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    ostr << "ALTER SHARD ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(shard_name);

    switch (command)
    {
        case AlterShardCommand::ReplaceReplicas:
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
            for (size_t c = 0; c < replica_replace_clauses.size(); ++c)
            {
                if (c)
                    ostr << ", REPLACE ";
                else
                    ostr << " REPLACE ";
                const auto & cl = replica_replace_clauses[c];
                format_list(cl.from_collections);
                ostr << " TO ";
                format_list(cl.to_collections);
            }
            if (!shard_definition_properties.empty())
            {
                ostr << " MODIFY PROPERTIES (";
                for (size_t i = 0; i < shard_definition_properties.size(); ++i)
                {
                    if (i)
                        ostr << ", ";
                    const auto & ch = shard_definition_properties[i];
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
        case AlterShardCommand::AddReplica:
            ostr << " ADD REPLICA " << backQuoteIfNeed(replica_name);
            break;
        case AlterShardCommand::DropReplica:
            ostr << " DROP REPLICA " << backQuoteIfNeed(replica_name);
            break;
        case AlterShardCommand::ModifyReplica:
            ostr << " MODIFY REPLICA " << backQuoteIfNeed(replica_name) << " PROPERTIES (";
            for (size_t i = 0; i < replica_properties.size(); ++i)
            {
                if (i)
                    ostr << ", ";
                const auto & ch = replica_properties[i];
                formatSettingName(ch.name, ostr);
                if (s.show_secrets)
                    ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
                else
                    ostr << " = '[HIDDEN]'";
            }
            ostr << ")";
            break;
        case AlterShardCommand::RenameReplica:
            ostr << " RENAME REPLICA " << backQuoteIfNeed(replica_name) << " TO " << backQuoteIfNeed(rename_replica_to);
            break;
        case AlterShardCommand::ModifyShardProperties:
            ostr << " MODIFY PROPERTIES (";
            for (size_t i = 0; i < shard_definition_properties.size(); ++i)
            {
                if (i)
                    ostr << ", ";
                const auto & ch = shard_definition_properties[i];
                formatSettingName(ch.name, ostr);
                if (s.show_secrets)
                    ostr << " = " << applyVisitor(FieldVisitorToString(), ch.value);
                else
                    ostr << " = '[HIDDEN]'";
            }
            ostr << ")";
            break;
    }

    formatOnCluster(ostr, s);
}

}
