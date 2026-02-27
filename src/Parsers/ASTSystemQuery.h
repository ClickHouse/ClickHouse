#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>
#include <Parsers/SyncReplicaMode.h>
#include <Server/ServerType.h>

#include "config.h"

#if USE_XRAY
#include <Interpreters/InstrumentationManager.h>
#include <variant>
#endif

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
        CLEAR_DNS_CACHE,
        CLEAR_CONNECTIONS_CACHE,
        PREWARM_MARK_CACHE,
        PREWARM_PRIMARY_INDEX_CACHE,
        CLEAR_MARK_CACHE,
        CLEAR_PRIMARY_INDEX_CACHE,
        CLEAR_UNCOMPRESSED_CACHE,
        CLEAR_INDEX_MARK_CACHE,
        CLEAR_INDEX_UNCOMPRESSED_CACHE,
        CLEAR_VECTOR_SIMILARITY_INDEX_CACHE,
        CLEAR_TEXT_INDEX_DICTIONARY_CACHE,
        CLEAR_TEXT_INDEX_HEADER_CACHE,
        CLEAR_TEXT_INDEX_POSTINGS_CACHE,
        CLEAR_TEXT_INDEX_CACHES,
        CLEAR_MMAP_CACHE,
        CLEAR_QUERY_CONDITION_CACHE,
        CLEAR_QUERY_CACHE,
        CLEAR_COMPILED_EXPRESSION_CACHE,
        CLEAR_ICEBERG_METADATA_CACHE,
        CLEAR_FILESYSTEM_CACHE,
        CLEAR_DISTRIBUTED_CACHE,
        CLEAR_DISK_METADATA_CACHE,
        CLEAR_PAGE_CACHE,
        CLEAR_SCHEMA_CACHE,
        CLEAR_FORMAT_SCHEMA_CACHE,
        CLEAR_S3_CLIENT_CACHE,
        STOP_LISTEN,
        START_LISTEN,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        RESTORE_REPLICA,
        RESTORE_DATABASE_REPLICA,
        WAIT_LOADING_PARTS,
        DROP_REPLICA,
        DROP_DATABASE_REPLICA,
        DROP_CATALOG_REPLICA,
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
        RELOAD_DELTA_KERNEL_TRACING,
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
        STOP_REPLICATED_DDL_QUERIES,
        START_REPLICATED_DDL_QUERIES,
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
        ALLOCATE_MEMORY,
        FREE_MEMORY,
        WAIT_FAILPOINT,
        NOTIFY_FAILPOINT,
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
        START_REPLICATED_VIEW,
        STOP_VIEW,
        STOP_VIEWS,
        STOP_REPLICATED_VIEW,
        CANCEL_VIEW,
        TEST_VIEW,
        LOAD_PRIMARY_KEY,
        UNLOAD_PRIMARY_KEY,
        STOP_VIRTUAL_PARTS_UPDATE,
        START_VIRTUAL_PARTS_UPDATE,
        STOP_REDUCE_BLOCKING_PARTS,
        START_REDUCE_BLOCKING_PARTS,
        UNLOCK_SNAPSHOT,
        RECONNECT_ZOOKEEPER,
        INSTRUMENT_ADD,
        INSTRUMENT_REMOVE,
        RESET_DDL_WORKER,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    ASTPtr database;
    ASTPtr table;
    bool if_exists = false;
    ASTPtr query_settings;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    String target_model;
    String target_function;
    String replica;
    String shard;
    String zk_name;
    String full_replica_zk_path;
    String replica_zk_path;
    bool is_drop_whole_replica{};
    bool with_tables{false};
    String storage_policy;
    String volume;
    String disk;
    UInt64 seconds{};
    UInt64 untracked_memory_size{};

    std::optional<String> query_result_cache_tag;

    String filesystem_cache_name;
    String distributed_cache_server_id;
    bool distributed_cache_drop_connections = false;

    std::string key_to_drop;
    std::optional<size_t> offset_to_drop;

    String backup_name;
    ASTPtr backup_source; /// SYSTEM UNFREEZE SNAPSHOT `backup_name` FROM `backup_source`

    String schema_cache_storage;

    String schema_cache_format;

    String fail_point_name;

    enum class FailPointAction
    {
        UNSPECIFIED,
        PAUSE,
        RESUME
    };
    FailPointAction fail_point_action = FailPointAction::UNSPECIFIED;

    String delta_kernel_tracing_level;

    SyncReplicaMode sync_replica_mode = SyncReplicaMode::DEFAULT;

    std::vector<String> src_replicas;

    std::vector<std::pair<String, String>> tables;

    ServerType server_type;

#if USE_XRAY
    /// For SYSTEM INSTRUMENT ADD/REMOVE
    using InstrumentParameter = std::variant<String, Int64, Float64>;
    String instrumentation_function_name;
    String instrumentation_handler_name;
    Instrumentation::EntryType instrumentation_entry_type;
    std::optional<std::variant<UInt64, Instrumentation::All, String>> instrumentation_point;
    std::vector<InstrumentParameter> instrumentation_parameters;
    String instrumentation_subquery;
#endif

    /// For SYSTEM TEST VIEW <name> (SET FAKE TIME <time> | UNSET FAKE TIME).
    /// Unix time.
    std::optional<Int64> fake_time_for_view;

    String getID(char) const override { return "SYSTEM query"; }

    ASTPtr clone() const override
    {
        auto res = make_intrusive<ASTSystemQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }
        if (query_settings) { res->query_settings = query_settings->clone(); res->children.push_back(res->query_settings); }
        if (backup_source) { res->backup_source = backup_source->clone(); res->children.push_back(res->backup_source); }

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
