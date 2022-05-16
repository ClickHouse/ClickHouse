#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>

#include "config_core.h"


namespace DB
{

class ASTSystemQuery : public IAST, public ASTQueryWithOnCluster
{
public:

    enum class Type : UInt64
    {
        UNKNOWN,
        SHUTDOWN,
        KILL,
        SUSPEND,
        DROP_DNS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
        DROP_INDEX_MARK_CACHE,
        DROP_INDEX_UNCOMPRESSED_CACHE,
        DROP_MMAP_CACHE,
#if USE_EMBEDDED_COMPILER
        DROP_COMPILED_EXPRESSION_CACHE,
#endif
        DROP_FILESYSTEM_CACHE,
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        RESTORE_REPLICA,
        DROP_REPLICA,
        SYNC_REPLICA,
        SYNC_DATABASE_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_MODEL,
        RELOAD_MODELS,
        RELOAD_FUNCTION,
        RELOAD_FUNCTIONS,
        RELOAD_EMBEDDED_DICTIONARIES,
        RELOAD_CONFIG,
        RELOAD_SYMBOLS,
        RESTART_DISK,
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
        START_THREAD_FUZZER,
        STOP_THREAD_FUZZER,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    ASTPtr database;
    ASTPtr table;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    String target_model;
    String target_function;
    String replica;
    String replica_zk_path;
    bool is_drop_whole_replica{};
    String storage_policy;
    String volume;
    String disk;
    UInt64 seconds{};

    String filesystem_cache_path;

    String getID(char) const override { return "SYSTEM query"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTSystemQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }

        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTSystemQuery>(clone(), params.default_database);
    }

    virtual QueryKind getQueryKind() const override { return QueryKind::System; }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
