#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>
#include <Parsers/SyncReplicaMode.h>
#include <Server/ServerType.h>

#include "config.h"


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
        DROP_CONNECTIONS_CACHE,
        PREWARM_MARK_CACHE,
        PREWARM_PRIMARY_INDEX_CACHE,
        DROP_MARK_CACHE,
        DROP_PRIMARY_INDEX_CACHE,
        DROP_UNCOMPRESSED_CACHE,
        DROP_INDEX_MARK_CACHE,
        DROP_INDEX_UNCOMPRESSED_CACHE,
        DROP_MMAP_CACHE,
        DROP_QUERY_CACHE,
        DROP_COMPILED_EXPRESSION_CACHE,
        DROP_FILESYSTEM_CACHE,
        DROP_DISK_METADATA_CACHE,
        DROP_PAGE_CACHE,
        DROP_SCHEMA_CACHE,
        DROP_FORMAT_SCHEMA_CACHE,
        DROP_S3_CLIENT_CACHE,
        STOP_LISTEN,
        START_LISTEN,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        RESTORE_REPLICA,
        WAIT_LOADING_PARTS,
        DROP_REPLICA,
        DROP_DATABASE_REPLICA,
        JEMALLOC_PURGE,
        JEMALLOC_ENABLE_PROFILE,
        JEMALLOC_DISABLE_PROFILE,
        JEMALLOC_FLUSH_PROFILE,
        SYNC_REPLICA,
        SYNC_DATABASE_REPLICA,
        SYNC_TRANSACTION_LOG,
        SYNC_FILE_CACHE,
        REPLICA_READY,
        REPLICA_UNREADY,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_MODEL,
        RELOAD_MODELS,
        RELOAD_FUNCTION,
        RELOAD_FUNCTIONS,
        RELOAD_EMBEDDED_DICTIONARIES,
        RELOAD_CONFIG,
        RELOAD_USERS,
        RELOAD_ASYNCHRONOUS_METRICS,
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
        FLUSH_ASYNC_INSERT_QUEUE,
        STOP_DISTRIBUTED_SENDS,
        START_DISTRIBUTED_SENDS,
        START_THREAD_FUZZER,
        STOP_THREAD_FUZZER,
        UNFREEZE,
        ENABLE_FAILPOINT,
        DISABLE_FAILPOINT,
        WAIT_FAILPOINT,
        SYNC_FILESYSTEM_CACHE,
        STOP_PULLING_REPLICATION_LOG,
        START_PULLING_REPLICATION_LOG,
        STOP_CLEANUP,
        START_CLEANUP,
        RESET_COVERAGE,
        REFRESH_VIEW,
        WAIT_VIEW,
        START_VIEW,
        START_VIEWS,
        STOP_VIEW,
        STOP_VIEWS,
        CANCEL_VIEW,
        TEST_VIEW,
        LOAD_PRIMARY_KEY,
        UNLOAD_PRIMARY_KEY,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    ASTPtr database;
    ASTPtr table;
    ASTPtr query_settings;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    String target_model;
    String target_function;
    String replica;
    String shard;
    String replica_zk_path;
    bool is_drop_whole_replica{};
    String storage_policy;
    String volume;
    String disk;
    UInt64 seconds{};

    std::optional<String> query_cache_tag;

    String filesystem_cache_name;
    std::string key_to_drop;
    std::optional<size_t> offset_to_drop;

    String backup_name;

    String schema_cache_storage;

    String schema_cache_format;

    String fail_point_name;

    SyncReplicaMode sync_replica_mode = SyncReplicaMode::DEFAULT;

    std::vector<String> src_replicas;

    ServerType server_type;

    /// For SYSTEM TEST VIEW <name> (SET FAKE TIME <time> | UNSET FAKE TIME).
    /// Unix time.
    std::optional<Int64> fake_time_for_view;

    String getID(char) const override { return "SYSTEM query"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTSystemQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }
        if (query_settings) { res->query_settings = query_settings->clone(); res->children.push_back(res->query_settings); }

        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTSystemQuery>(clone(), params.default_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::System; }

protected:

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
