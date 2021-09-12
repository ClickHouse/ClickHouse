#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM ";
    settings.ostr << type << (settings.hilite ? hilite_none : "");

    auto print_database_table = [&]
    {
        settings.ostr << " ";
        if (!database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(table)
                      << (settings.hilite ? hilite_none : "");
    };

    auto print_drop_replica = [&]
    {
        settings.ostr << " " << quoteString(replica);
        if (!table.empty())
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
        else if (!database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM DATABASE "
                          << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(database)
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
        || type == Type::START_DISTRIBUTED_SENDS)
    {
        if (!table.empty())
            print_database_table();
        else if (!volume.empty())
            print_on_volume();
    }
    else if (  type == Type::RESTART_REPLICA
            || type == Type::RESTORE_REPLICA
            || type == Type::SYNC_REPLICA
            || type == Type::FLUSH_DISTRIBUTED
            || type == Type::RELOAD_DICTIONARY)
    {
        print_database_table();
    }
    else if (type == Type::DROP_REPLICA)
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
}


}
