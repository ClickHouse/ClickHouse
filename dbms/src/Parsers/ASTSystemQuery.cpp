#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int NOT_IMPLEMENTED;
}


const char * ASTSystemQuery::typeToString(Type type)
{
    switch (type)
    {
        case Type::SHUTDOWN:
            return "SHUTDOWN";
        case Type::KILL:
            return "KILL";
        case Type::DROP_DNS_CACHE:
            return "DROP DNS CACHE";
        case Type::DROP_MARK_CACHE:
            return "DROP MARK CACHE";
        case Type::DROP_UNCOMPRESSED_CACHE:
            return "DROP UNCOMPRESSED CACHE";
        case Type::STOP_LISTEN_QUERIES:
            return "STOP LISTEN QUERIES";
        case Type::START_LISTEN_QUERIES:
            return "START LISTEN QUERIES";
        case Type::RESTART_REPLICAS:
            return "RESTART REPLICAS";
        case Type::RESTART_REPLICA:
            return "RESTART REPLICA";
        case Type::SYNC_REPLICA:
            return "SYNC REPLICA";
        case Type::RELOAD_DICTIONARY:
            return "RELOAD DICTIONARY";
        case Type::RELOAD_DICTIONARIES:
            return "RELOAD DICTIONARIES";
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            return "RELOAD EMBEDDED DICTIONARIES";
        case Type::RELOAD_CONFIG:
            return "RELOAD CONFIG";
        case Type::STOP_MERGES:
            return "STOP MERGES";
        case Type::START_MERGES:
            return "START MERGES";
        case Type::STOP_FETCHES:
            return "STOP FETCHES";
        case Type::START_FETCHES:
            return "START FETCHES";
        case Type::STOP_REPLICATED_SENDS:
            return "STOP REPLICATED SENDS";
        case Type::START_REPLICATEDS_SENDS:
            return "START REPLICATED SENDS";
        case Type::STOP_REPLICATION_QUEUES:
            return "STOP REPLICATION QUEUES";
        case Type::START_REPLICATION_QUEUES:
            return "START REPLICATION QUEUES";
        default:
            throw Exception("Unknown SYSTEM query command", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM " << (settings.hilite ? hilite_none : "");
    settings.ostr << typeToString(type);

    auto print_database_table = [&] ()
    {
        settings.ostr << " ";

        if (!target_database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(target_database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }

        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(target_table)
                      << (settings.hilite ? hilite_none : "");
    };

    if (   type == Type::STOP_MERGES
        || type == Type::START_MERGES
        || type == Type::STOP_FETCHES
        || type == Type::START_FETCHES
        || type == Type::STOP_REPLICATED_SENDS
        || type == Type::START_REPLICATEDS_SENDS
        || type == Type::STOP_REPLICATION_QUEUES
        || type == Type::START_REPLICATION_QUEUES)
    {
        if (!target_table.empty())
            print_database_table();
    }
    else if (type == Type::RESTART_REPLICA || type == Type::SYNC_REPLICA)
    {
        print_database_table();
    }
    else if (type == Type::RELOAD_DICTIONARY)
        settings.ostr << " " << backQuoteIfNeed(target_dictionary);
}


}
