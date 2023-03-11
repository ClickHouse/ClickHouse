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

void ASTSystemQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("SYSTEM ");
    out.writeKeyword(typeToString(type));

    auto print_database_table = [&]
    {
        out.ostr << " ";
        if (database)
        {
            out.writeProbablyBackQuotedIdentifier(getDatabase());
            out.ostr << ".";
        }
        out.writeProbablyBackQuotedIdentifier(getTable());
    };

    auto print_drop_replica = [&]
    {
        out.ostr << " " << quoteString(replica);
        if (table)
        {
            out.writeKeyword(" FROM TABLE");
            print_database_table();
        }
        else if (!replica_zk_path.empty())
        {
            out.writeKeyword(" FROM ZKPATH ");
        }
        else if (database)
        {
            out.writeKeyword(" FROM DATABASE ");
            out.writeProbablyBackQuotedIdentifier(getDatabase());
        }
    };

    auto print_on_volume = [&]
    {
        out.writeKeyword(" ON VOLUME ");
        out.writeProbablyBackQuotedIdentifier(storage_policy);
        out.ostr << ".";
        out.writeProbablyBackQuotedIdentifier(volume);
    };

    auto print_identifier = [&](const String & identifier)
    {
        out.ostr << " ";
        out.writeProbablyBackQuotedIdentifier(identifier);
    };

    if (!cluster.empty())
        formatOnCluster(out);

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
        || type == Type::START_DISTRIBUTED_SENDS)
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
        out.writeKeyword(" FOR ");
        out.ostr << seconds;
        out.writeKeyword(" SECONDS");
    }
    else if (type == Type::DROP_FILESYSTEM_CACHE)
    {
        if (!filesystem_cache_path.empty())
            out.ostr << " " << filesystem_cache_path;
    }
    else if (type == Type::UNFREEZE)
    {
        out.writeProbablyBackQuotedIdentifier(backup_name);
    }
}


}
