#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>


namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}


namespace DB
{


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
        case Type::SYNC_REPLICA:
            return "SYNC REPLICA";
        case Type::RELOAD_DICTIONARY:
            return "RELOAD DICTIONARY";
        case Type::RELOAD_DICTIONARIES:
            return "RELOAD DICTIONARIES";
        case Type::STOP_MERGES:
            return "STOP MERGES";
        case Type::START_MERGES:
            return "START MERGES";
        case Type::STOP_REPLICATION_QUEUES:
            return "STOP REPLICATION QUEUES";
        case Type::START_REPLICATION_QUEUES:
            return "START REPLICATION QUEUES";
        default:
            throw Exception("Unknown SYSTEM query command", ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


}