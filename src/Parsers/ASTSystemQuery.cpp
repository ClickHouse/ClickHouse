#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

#include <magic_enum.hpp>

namespace DB
{

namespace
{
    std::vector<std::string> getTypeIndexToTypeName()
    {
        constexpr std::size_t types_size = magic_enum::enum_count<ASTSystemQuery::Type>();

        std::vector<std::string> type_index_to_type_name;
        type_index_to_type_name.resize(types_size);

        auto entries = magic_enum::enum_entries<ASTSystemQuery::Type>();
        for (const auto & [entry, str] : entries)
        {
            auto str_copy = String(str);
            std::replace(str_copy.begin(), str_copy.end(), '_', ' ');
            type_index_to_type_name[static_cast<UInt64>(entry)] = std::move(str_copy);
        }

        return type_index_to_type_name;
    }
}

const char * ASTSystemQuery::typeToString(Type type)
{
    /** During parsing if SystemQuery is not parsed properly it is added to Expected variants as description check IParser.h.
      * Description string must be statically allocated.
      */
    static std::vector<std::string> type_index_to_type_name = getTypeIndexToTypeName();
    const auto & type_name = type_index_to_type_name[static_cast<UInt64>(type)];
    return type_name.data();
}

String ASTSystemQuery::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTSystemQuery::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTSystemQuery::setDatabase(const String & name)
{
    if (database)
    {
        std::erase(children, database);
        database.reset();
    }

    if (!name.empty())
    {
        database = std::make_shared<ASTIdentifier>(name);
        children.push_back(database);
    }
}

void ASTSystemQuery::setTable(const String & name)
{
    if (table)
    {
        std::erase(children, table);
        table.reset();
    }

    if (!name.empty())
    {
        table = std::make_shared<ASTIdentifier>(name);
        children.push_back(table);
    }
}

void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM ";
    settings.ostr << typeToString(type) << (settings.hilite ? hilite_none : "");

    auto print_database_table = [&]
    {
        settings.ostr << " ";
        if (database)
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getDatabase())
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getTable())
                      << (settings.hilite ? hilite_none : "");
    };

    auto print_drop_replica = [&]
    {
        settings.ostr << " " << quoteString(replica);
        if (!shard.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM SHARD "
                          << (settings.hilite ? hilite_none : "") << quoteString(shard);
        }

        if (table)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM TABLE"
                          << (settings.hilite ? hilite_none : "");
            print_database_table();
        }
        else if (!replica_zk_path.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM ZKPATH "
                          << (settings.hilite ? hilite_none : "") << quoteString(replica_zk_path);
        }
        else if (database)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM DATABASE "
                          << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getDatabase())
                          << (settings.hilite ? hilite_none : "");
        }
    };

    auto print_on_volume = [&]
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON VOLUME "
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(storage_policy)
                      << (settings.hilite ? hilite_none : "")
                      << "."
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(volume)
                      << (settings.hilite ? hilite_none : "");
    };

    auto print_identifier = [&](const String & identifier)
    {
        settings.ostr << " " << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(identifier)
                      << (settings.hilite ? hilite_none : "");
    };

    if (!cluster.empty())
        formatOnCluster(settings);

    if (   type == Type::STOP_MERGES
        || type == Type::START_MERGES
        || type == Type::STOP_TTL_MERGES
        || type == Type::START_TTL_MERGES
        || type == Type::STOP_MOVES
        || type == Type::START_MOVES
        || type == Type::STOP_FETCHES
        || type == Type::START_FETCHES
        || type == Type::STOP_REPLICATED_SENDS
        || type == Type::START_REPLICATED_SENDS
        || type == Type::STOP_REPLICATION_QUEUES
        || type == Type::START_REPLICATION_QUEUES
        || type == Type::STOP_DISTRIBUTED_SENDS
        || type == Type::START_DISTRIBUTED_SENDS
        || type == Type::STOP_PULLING_REPLICATION_LOG
        || type == Type::START_PULLING_REPLICATION_LOG)
    {
        if (table)
            print_database_table();
        else if (!volume.empty())
            print_on_volume();
    }
    else if (  type == Type::RESTART_REPLICA
            || type == Type::RESTORE_REPLICA
            || type == Type::SYNC_REPLICA
            || type == Type::WAIT_LOADING_PARTS
            || type == Type::FLUSH_DISTRIBUTED
            || type == Type::RELOAD_DICTIONARY
            || type == Type::RELOAD_MODEL
            || type == Type::RELOAD_FUNCTION
            || type == Type::RESTART_DISK)
    {
        if (table)
            print_database_table();
        else if (!target_model.empty())
            print_identifier(target_model);
        else if (!target_function.empty())
            print_identifier(target_function);
        else if (!disk.empty())
            print_identifier(disk);

        if (sync_replica_mode != SyncReplicaMode::DEFAULT)
            settings.ostr << ' ' << (settings.hilite ? hilite_keyword : "") << magic_enum::enum_name(sync_replica_mode)
                          << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::SYNC_DATABASE_REPLICA)
    {
        print_identifier(database->as<ASTIdentifier>()->name());
    }
    else if (type == Type::DROP_REPLICA || type == Type::DROP_DATABASE_REPLICA)
    {
        print_drop_replica();
    }
    else if (type == Type::SUSPEND)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOR "
            << (settings.hilite ? hilite_none : "") << seconds
            << (settings.hilite ? hilite_keyword : "") << " SECOND"
            << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::DROP_FILESYSTEM_CACHE)
    {
        if (!filesystem_cache_name.empty())
        {
            settings.ostr << (settings.hilite ? hilite_none : "") << " " << filesystem_cache_name;
            if (!key_to_drop.empty())
            {
                settings.ostr << (settings.hilite ? hilite_none : "") << " KEY " << key_to_drop;
                if (offset_to_drop.has_value())
                    settings.ostr << (settings.hilite ? hilite_none : "") << " OFFSET " << offset_to_drop.value();
            }
        }
    }
    else if (type == Type::UNFREEZE)
    {
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(backup_name);
    }
    else if (type == Type::SYNC_FILE_CACHE)
    {
        settings.ostr << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::START_LISTEN || type == Type::STOP_LISTEN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " "
            << ServerType::serverTypeToString(server_type.type) << (settings.hilite ? hilite_none : "");

        if (server_type.type == ServerType::Type::CUSTOM)
        {
            settings.ostr << " " << quoteString(server_type.custom_name);
        }

        bool comma = false;

        if (!server_type.exclude_types.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                << " EXCEPT" << (settings.hilite ? hilite_none : "");

            for (auto cur_type : server_type.exclude_types)
            {
                if (cur_type == ServerType::Type::CUSTOM)
                    continue;

                if (comma)
                    settings.ostr << ",";
                else
                    comma = true;

                settings.ostr << (settings.hilite ? hilite_keyword : "") << " "
                    << ServerType::serverTypeToString(cur_type) << (settings.hilite ? hilite_none : "");
            }

            if (server_type.exclude_types.contains(ServerType::Type::CUSTOM))
            {
                for (const auto & cur_name : server_type.exclude_custom_names)
                {
                    if (comma)
                        settings.ostr << ",";
                    else
                        comma = true;

                    settings.ostr << (settings.hilite ? hilite_keyword : "") << " "
                        << ServerType::serverTypeToString(ServerType::Type::CUSTOM) << (settings.hilite ? hilite_none : "");

                    settings.ostr << " " << quoteString(cur_name);
                }
            }
        }

    }
}


}
