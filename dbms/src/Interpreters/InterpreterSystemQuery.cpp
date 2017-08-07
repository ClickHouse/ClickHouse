#include <Interpreters/InterpreterSystemQuery.h>
#include <Interpreters/DNSCache.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/typeid_cast.h>
#include <csignal>
#include <bits/signum.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
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
            DNSCache::instance().dropCache();
            break;
        case Type::DROP_MARK_CACHE:
            context.dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            context.dropUncompressedCache();
            break;
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
        case Type::RESTART_REPLICAS:
        case Type::SYNC_REPLICA:
        case Type::RELOAD_DICTIONARY:
        case Type::RELOAD_DICTIONARIES:
        case Type::STOP_MERGES:
        case Type::START_MERGES:
        case Type::STOP_REPLICATION_QUEUES:
        case Type::START_REPLICATION_QUEUES:
            throw Exception(String(ASTSystemQuery::typeToString(query.type)) + " is not supported yet", ErrorCodes::NOT_IMPLEMENTED);
        default:
            throw Exception("Unknown type of SYSTEM query", ErrorCodes::BAD_ARGUMENTS);
    }

    return BlockIO();
}


}