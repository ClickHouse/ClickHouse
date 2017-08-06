#include <Interpreters/InterpreterSystemQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/typeid_cast.h>
#include <csignal>


namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = typeid_cast<ASTSystemQuery &>(*query_ptr);

    using Type = ASTSystemQuery::Type;

    switch (query.type)
    {
        case Type::SHUTDOWN:
            if (kill(0, SIGTERM))
                throw Exception("System call kill() failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::KILL:
            if (kill(0, SIGKILL))
                throw Exception("System call kill() failed", ErrorCodes::CANNOT_KILL);
            break;
        case Type::DROP_DNS_CACHE:
            break;
        case Type::DROP_MARK_CACHE:
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            break;
        case Type::STOP_LISTEN_QUERIES:break;
        case Type::START_LISTEN_QUERIES:break;
        case Type::RESTART_REPLICAS:break;
        case Type::SYNC_REPLICA:break;
        case Type::RELOAD_DICTIONARY:break;
        case Type::RELOAD_DICTIONARIES:break;
        case Type::STOP_MERGES:break;
        case Type::START_MERGES:break;
        case Type::STOP_REPLICATION_QUEUES:break;
        case Type::START_REPLICATION_QUEUES:break;
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}


}