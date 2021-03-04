#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{

class ASTSystemQuery : public IAST, public ASTQueryWithOnCluster
{
public:

    enum class Type
    {
        UNKNOWN,
        SHUTDOWN,
        KILL,
        DROP_DNS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
#if USE_EMBEDDED_COMPILER
        DROP_COMPILED_EXPRESSION_CACHE,
#endif
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        DROP_REPLICA,
        SYNC_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_EMBEDDED_DICTIONARIES,
        RELOAD_CONFIG,
        STOP_MERGES,
        START_MERGES,
        STOP_TTL_MERGES,
        START_TTL_MERGES,
        STOP_FETCHES,
        START_FETCHES,
        STOP_MOVES,
        START_MOVES,
        STOP_REPLICATED_SENDS,
        START_REPLICATED_SENDS,
        STOP_REPLICATION_QUEUES,
        START_REPLICATION_QUEUES,
        FLUSH_LOGS,
        FLUSH_DISTRIBUTED,
        STOP_DISTRIBUTED_SENDS,
        START_DISTRIBUTED_SENDS,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    String target_dictionary;
    String database;
    String table;
    String replica;
    String replica_zk_path;
    bool is_drop_whole_replica;
    String storage_policy;
    String volume;

    String getID(char) const override { return "SYSTEM query"; }

    ASTPtr clone() const override { return std::make_shared<ASTSystemQuery>(*this); }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTSystemQuery>(clone(), new_database);
    }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
