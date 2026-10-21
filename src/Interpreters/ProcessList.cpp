#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Interpreters/CancellationChecker.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/queryNormalization.h>
#include <Processors/Executors/Runtime/IExecutor.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/OvercommitTracker.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/logger_useful.h>
#include <Common/saturatedDuration.h>
#include <chrono>
#include <memory>
#include <mutex>


namespace CurrentMetrics
{
    extern const Metric Query;
    extern const Metric QueryNonInternal;
    extern const Metric WaitingQuery;
}

namespace ProfileEvents
{
    extern const Event UserThrottlerBytes;
    extern const Event UserThrottlerSleepMicroseconds;
    extern const Event AllUsersThrottlerBytes;
    extern const Event AllUsersThrottlerSleepMicroseconds;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_concurrent_queries_for_all_users;
    extern const SettingsUInt64 max_concurrent_queries_for_user;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsUInt64 max_memory_usage;
    extern const SettingsUInt64 max_memory_usage_for_user;
    extern const SettingsUInt64 max_network_bandwidth_for_all_users;
    extern const SettingsUInt64 max_network_bandwidth_for_user;
    extern const SettingsUInt64 max_temporary_data_on_disk_size_for_user;
    extern const SettingsUInt64 memory_usage_overcommit_max_wait_microseconds;
    extern const SettingsUInt64 memory_overcommit_ratio_denominator_for_user;
    extern const SettingsUInt64 max_temporary_data_on_disk_size_for_query;
    extern const SettingsUInt64 priority;
    extern const SettingsMilliseconds queue_max_wait_ms;
    extern const SettingsBool replace_running_query;
    extern const SettingsMilliseconds replace_running_query_max_wait_ms;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsBool trace_profile_events;
    extern const SettingsString trace_profile_events_list;
    extern const SettingsMilliseconds low_priority_query_wait_time_ms;
    extern const SettingsUInt64 reserve_memory;
    extern const SettingsUInt64 min_bytes_to_spill;
    extern const SettingsMilliseconds workload_admission_timeout_ms;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
}


/// Should we execute the query even if max_concurrent_queries limit is exhausted
static bool isUnlimitedQuery(const IAST * ast)
{
    if (!ast)
        return false;

    /// It is KILL QUERY or an async insert flush query
    if (ast->as<ASTKillQueryQuery>() || ast->getQueryKind() == IAST::QueryKind::AsyncInsertFlush)
        return true;

    /// It is SELECT FROM system.processes
    /// NOTE: This is very rough check.
    /// False negative: USE system; SELECT * FROM processes;
    /// False positive: SELECT * FROM system.processes CROSS JOIN (SELECT ...)

    if (const auto * ast_selects = ast->as<ASTSelectWithUnionQuery>())
    {
        if (!ast_selects->list_of_selects || ast_selects->list_of_selects->children.empty())
            return false;

        const auto * ast_select = ast_selects->list_of_selects->children[0]->as<ASTSelectQuery>();
        if (!ast_select)
            return false;

        if (auto database_and_table = getDatabaseAndTable(*ast_select, 0))
            return database_and_table->database == "system" && database_and_table->table == "processes";

        return false;
    }

    return false;
}

/// A subquery anywhere in a statement (e.g. a scalar subquery in an engine argument) may be executed
/// while the statement is analyzed. So may the body of a SQL user-defined function, and some functions
/// read data when evaluated: a dictionary function loads the dictionary, `file` reads a file, and
/// `joinGet` reads a `Join` table.
static bool hasSubquery(const IAST & ast)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>() || ast.as<ASTSelectQuery>())
        return true;
    if (const auto * function = ast.as<ASTFunction>())
    {
        if (function->name.starts_with("dict") || function->name == "file" || function->name.starts_with("joinGet")
            || UserDefinedSQLFunctionFactory::instance().has(function->name))
            return true;
    }
    for (const auto & child : ast.children)
        if (child && hasSubquery(*child))
            return true;
    /// Not stored in `children`.
    if (const auto * ttl = ast.as<ASTTTLElement>())
    {
        for (const auto & expressions : {ttl->group_by_key, ttl->group_by_assignments})
            for (const auto & expression : expressions)
                if (expression && hasSubquery(*expression))
                    return true;
        if (ttl->recompression_codec && hasSubquery(*ttl->recompression_codec))
            return true;
    }
    return false;
}

/// SYSTEM commands that change only server state or metadata, or wait for work that runs elsewhere
/// (background merges, fetches, refreshes, log flushes). A new command has to be classified here.
static bool isMetadataSystemQuery(const ASTSystemQuery & query)
{
    switch (query.type)
    {
        case ASTSystemQuery::Type::SHUTDOWN:
        case ASTSystemQuery::Type::KILL:
        case ASTSystemQuery::Type::SUSPEND:
        case ASTSystemQuery::Type::CLEAR_DNS_CACHE:
        case ASTSystemQuery::Type::CLEAR_CONNECTIONS_CACHE:
        case ASTSystemQuery::Type::CLEAR_MARK_CACHE:
        case ASTSystemQuery::Type::CLEAR_PRIMARY_INDEX_CACHE:
        case ASTSystemQuery::Type::CLEAR_STATISTICS_CACHE:
        case ASTSystemQuery::Type::CLEAR_UNCOMPRESSED_CACHE:
        case ASTSystemQuery::Type::CLEAR_INDEX_MARK_CACHE:
        case ASTSystemQuery::Type::CLEAR_INDEX_UNCOMPRESSED_CACHE:
        case ASTSystemQuery::Type::CLEAR_VECTOR_SIMILARITY_INDEX_CACHE:
        case ASTSystemQuery::Type::CLEAR_TEXT_INDEX_TOKENS_CACHE:
        case ASTSystemQuery::Type::CLEAR_TEXT_INDEX_HEADER_CACHE:
        case ASTSystemQuery::Type::CLEAR_TEXT_INDEX_POSTINGS_CACHE:
        case ASTSystemQuery::Type::CLEAR_TEXT_INDEX_CACHES:
        case ASTSystemQuery::Type::CLEAR_COLUMNS_CACHE:
        case ASTSystemQuery::Type::CLEAR_MMAP_CACHE:
        case ASTSystemQuery::Type::CLEAR_QUERY_CONDITION_CACHE:
        case ASTSystemQuery::Type::CLEAR_ENCRYPTION_HEADERS_CACHE:
        case ASTSystemQuery::Type::CLEAR_QUERY_CACHE:
        case ASTSystemQuery::Type::CLEAR_COMPILED_EXPRESSION_CACHE:
        case ASTSystemQuery::Type::CLEAR_ICEBERG_METADATA_CACHE:
        case ASTSystemQuery::Type::CLEAR_PAIMON_METADATA_CACHE:
        case ASTSystemQuery::Type::CLEAR_PARQUET_METADATA_CACHE:
        case ASTSystemQuery::Type::CLEAR_POINT_IN_POLYGON_CACHE:
        case ASTSystemQuery::Type::CLEAR_FILESYSTEM_CACHE:
        case ASTSystemQuery::Type::CLEAR_DISTRIBUTED_CACHE:
        case ASTSystemQuery::Type::CLEAR_DISK_METADATA_CACHE:
        case ASTSystemQuery::Type::CLEAR_PAGE_CACHE:
        case ASTSystemQuery::Type::CLEAR_SCHEMA_CACHE:
        case ASTSystemQuery::Type::CLEAR_FORMAT_SCHEMA_CACHE:
        case ASTSystemQuery::Type::CLEAR_AVRO_SCHEMA_CACHE:
        case ASTSystemQuery::Type::CLEAR_S3_CLIENT_CACHE:
        case ASTSystemQuery::Type::CLEAR_TIME_SERIES_CACHES:
        case ASTSystemQuery::Type::STOP_LISTEN:
        case ASTSystemQuery::Type::START_LISTEN:
        case ASTSystemQuery::Type::RESTORE_DATABASE_REPLICA:
        case ASTSystemQuery::Type::WAIT_LOADING_PARTS:
        case ASTSystemQuery::Type::WAIT_QUERY_RUNNER:
        case ASTSystemQuery::Type::DROP_REPLICA:
        case ASTSystemQuery::Type::DROP_DATABASE_REPLICA:
        case ASTSystemQuery::Type::DROP_CATALOG_REPLICA:
        case ASTSystemQuery::Type::JEMALLOC_PURGE:
        case ASTSystemQuery::Type::JEMALLOC_ENABLE_PROFILE:
        case ASTSystemQuery::Type::JEMALLOC_DISABLE_PROFILE:
        case ASTSystemQuery::Type::JEMALLOC_FLUSH_PROFILE:
        case ASTSystemQuery::Type::SYNC_REPLICA:
        case ASTSystemQuery::Type::SYNC_DATABASE_REPLICA:
        case ASTSystemQuery::Type::SYNC_TRANSACTION_LOG:
        case ASTSystemQuery::Type::SYNC_FILE_CACHE:
        case ASTSystemQuery::Type::REPLICA_READY:
        case ASTSystemQuery::Type::REPLICA_UNREADY:
        case ASTSystemQuery::Type::UNLOAD_DICTIONARY:
        case ASTSystemQuery::Type::UNLOAD_DICTIONARIES:
        case ASTSystemQuery::Type::RELOAD_FUNCTION:
        case ASTSystemQuery::Type::RELOAD_FUNCTIONS:
        case ASTSystemQuery::Type::RELOAD_CONFIG:
        case ASTSystemQuery::Type::RELOAD_USERS:
        case ASTSystemQuery::Type::RELOAD_ASYNCHRONOUS_METRICS:
        case ASTSystemQuery::Type::RELOAD_DELTA_KERNEL_TRACING:
        case ASTSystemQuery::Type::STOP_MERGES:
        case ASTSystemQuery::Type::START_MERGES:
        case ASTSystemQuery::Type::STOP_TTL_MERGES:
        case ASTSystemQuery::Type::START_TTL_MERGES:
        case ASTSystemQuery::Type::STOP_FETCHES:
        case ASTSystemQuery::Type::START_FETCHES:
        case ASTSystemQuery::Type::STOP_MOVES:
        case ASTSystemQuery::Type::START_MOVES:
        case ASTSystemQuery::Type::STOP_REPLICATED_SENDS:
        case ASTSystemQuery::Type::START_REPLICATED_SENDS:
        case ASTSystemQuery::Type::STOP_REPLICATION_QUEUES:
        case ASTSystemQuery::Type::START_REPLICATION_QUEUES:
        case ASTSystemQuery::Type::STOP_REPLICATED_DDL_QUERIES:
        case ASTSystemQuery::Type::START_REPLICATED_DDL_QUERIES:
        case ASTSystemQuery::Type::FLUSH_LOGS:
        case ASTSystemQuery::Type::FLUSH_ASYNC_INSERT_QUEUE:
        case ASTSystemQuery::Type::FLUSH_OBJECT_STORAGE_QUEUE:
        case ASTSystemQuery::Type::STOP_DISTRIBUTED_SENDS:
        case ASTSystemQuery::Type::START_DISTRIBUTED_SENDS:
        case ASTSystemQuery::Type::START_THREAD_FUZZER:
        case ASTSystemQuery::Type::STOP_THREAD_FUZZER:
        case ASTSystemQuery::Type::UNFREEZE:
        case ASTSystemQuery::Type::ENABLE_FAILPOINT:
        case ASTSystemQuery::Type::DISABLE_FAILPOINT:
        case ASTSystemQuery::Type::DISABLE_ALL_FAILPOINTS:
        case ASTSystemQuery::Type::FREE_MEMORY:
        case ASTSystemQuery::Type::WAIT_FAILPOINT:
        case ASTSystemQuery::Type::NOTIFY_FAILPOINT:
        case ASTSystemQuery::Type::SYNC_FILESYSTEM_CACHE:
        case ASTSystemQuery::Type::STOP_PULLING_REPLICATION_LOG:
        case ASTSystemQuery::Type::START_PULLING_REPLICATION_LOG:
        case ASTSystemQuery::Type::STOP_CLEANUP:
        case ASTSystemQuery::Type::START_CLEANUP:
        case ASTSystemQuery::Type::SCHEDULE_MERGE:
        case ASTSystemQuery::Type::SYNC_MERGES:
        case ASTSystemQuery::Type::RESET_COVERAGE:
        case ASTSystemQuery::Type::SET_COVERAGE_TEST:
        case ASTSystemQuery::Type::REFRESH_VIEW:
        case ASTSystemQuery::Type::WAIT_VIEW:
        case ASTSystemQuery::Type::START_VIEW:
        case ASTSystemQuery::Type::START_VIEWS:
        case ASTSystemQuery::Type::START_REPLICATED_VIEW:
        case ASTSystemQuery::Type::STOP_VIEW:
        case ASTSystemQuery::Type::STOP_VIEWS:
        case ASTSystemQuery::Type::STOP_REPLICATED_VIEW:
        case ASTSystemQuery::Type::PAUSE_VIEW:
        case ASTSystemQuery::Type::PAUSE_VIEWS:
        case ASTSystemQuery::Type::CANCEL_VIEW:
        case ASTSystemQuery::Type::TEST_VIEW:
        case ASTSystemQuery::Type::UNLOAD_PRIMARY_KEY:
        case ASTSystemQuery::Type::STOP_VIRTUAL_PARTS_UPDATE:
        case ASTSystemQuery::Type::START_VIRTUAL_PARTS_UPDATE:
        case ASTSystemQuery::Type::STOP_REDUCE_BLOCKING_PARTS:
        case ASTSystemQuery::Type::START_REDUCE_BLOCKING_PARTS:
        case ASTSystemQuery::Type::UNLOCK_SNAPSHOT:
        case ASTSystemQuery::Type::RECONNECT_ZOOKEEPER:
        case ASTSystemQuery::Type::WAIT_BLOBS_CLEANUP:
        case ASTSystemQuery::Type::INSTRUMENT_ADD:
        case ASTSystemQuery::Type::RESET_DDL_WORKER:
        case ASTSystemQuery::Type::RESET_FILELOG:
        case ASTSystemQuery::Type::STOP_ALL_BACKGROUND:
        case ASTSystemQuery::Type::START_ALL_BACKGROUND:
        case ASTSystemQuery::Type::PAUSE_ALL_BACKGROUND:
        case ASTSystemQuery::Type::CANCEL_ALL_BACKGROUND:
        case ASTSystemQuery::Type::REFRESH_ALL_BACKGROUND:
        case ASTSystemQuery::Type::STOP:
        case ASTSystemQuery::Type::START:
        case ASTSystemQuery::Type::PAUSE:
        case ASTSystemQuery::Type::CANCEL:
        case ASTSystemQuery::Type::REFRESH:
            return true;
        /// Read or send table data on the thread group of the query, or allocate memory.
        case ASTSystemQuery::Type::ALLOCATE_MEMORY:
        case ASTSystemQuery::Type::UNKNOWN:
        case ASTSystemQuery::Type::PREWARM_MARK_CACHE:
        case ASTSystemQuery::Type::PREWARM_PRIMARY_INDEX_CACHE:
        case ASTSystemQuery::Type::RESTART_REPLICAS:
        case ASTSystemQuery::Type::RESTART_REPLICA:
        case ASTSystemQuery::Type::RESTORE_REPLICA:
        case ASTSystemQuery::Type::RELOAD_DICTIONARY:
        case ASTSystemQuery::Type::RELOAD_DICTIONARIES:
        case ASTSystemQuery::Type::RELOAD_EMBEDDED_DICTIONARIES:
        case ASTSystemQuery::Type::RESTART_DISK:
        case ASTSystemQuery::Type::FLUSH_DISTRIBUTED:
        case ASTSystemQuery::Type::LOAD_PRIMARY_KEY:
        case ASTSystemQuery::Type::INSTRUMENT_REMOVE:
        case ASTSystemQuery::Type::END:
            return false;
    }
}

/// ALTER commands that change only metadata or delete data. A mutation only enqueues its work, which
/// runs under `mutation_workload`. A new command has to be classified here.
static bool isMetadataAlterCommand(const ASTAlterCommand & command)
{
    switch (command.type)
    {
        case ASTAlterCommand::ADD_COLUMN:
        case ASTAlterCommand::DROP_COLUMN:
        case ASTAlterCommand::MODIFY_COLUMN:
        case ASTAlterCommand::COMMENT_COLUMN:
        case ASTAlterCommand::RENAME_COLUMN:
        case ASTAlterCommand::MODIFY_ORDER_BY:
        case ASTAlterCommand::MODIFY_PROJECTION:
        case ASTAlterCommand::MODIFY_SAMPLE_BY:
        case ASTAlterCommand::MODIFY_TTL:
        case ASTAlterCommand::MODIFY_SETTING:
        case ASTAlterCommand::RESET_SETTING:
        case ASTAlterCommand::MODIFY_REFRESH:
        case ASTAlterCommand::REMOVE_TTL:
        case ASTAlterCommand::REMOVE_SAMPLE_BY:
        case ASTAlterCommand::ADD_INDEX:
        case ASTAlterCommand::DROP_INDEX:
        case ASTAlterCommand::ADD_CONSTRAINT:
        case ASTAlterCommand::DROP_CONSTRAINT:
        case ASTAlterCommand::MODIFY_CONSTRAINT:
        case ASTAlterCommand::ADD_PROJECTION:
        case ASTAlterCommand::DROP_PROJECTION:
        case ASTAlterCommand::ADD_STATISTICS:
        case ASTAlterCommand::DROP_STATISTICS:
        case ASTAlterCommand::MODIFY_STATISTICS:
        case ASTAlterCommand::DROP_PARTITION:
        case ASTAlterCommand::DROP_DETACHED_PARTITION:
        case ASTAlterCommand::FORGET_PARTITION:
        case ASTAlterCommand::UNFREEZE_PARTITION:
        case ASTAlterCommand::UNFREEZE_ALL:
        case ASTAlterCommand::MODIFY_DATABASE_SETTING:
        case ASTAlterCommand::MODIFY_DATABASE_COMMENT:
        case ASTAlterCommand::MODIFY_COMMENT:
        case ASTAlterCommand::MODIFY_SQL_SECURITY:
        case ASTAlterCommand::UNLOCK_SNAPSHOT:
            return true;
        /// Mutations: some engines (e.g. `Memory`, `Join`, `EmbeddedRocksDB`, `KeeperMap`) run them in the query.
        case ASTAlterCommand::MATERIALIZE_COLUMN:
        case ASTAlterCommand::REWRITE_PARTS:
        case ASTAlterCommand::MATERIALIZE_TTL:
        case ASTAlterCommand::MATERIALIZE_INDEX:
        case ASTAlterCommand::MATERIALIZE_PROJECTION:
        case ASTAlterCommand::MATERIALIZE_STATISTICS:
        case ASTAlterCommand::DELETE:
        case ASTAlterCommand::UPDATE:
        case ASTAlterCommand::APPLY_DELETED_MASK:
        case ASTAlterCommand::APPLY_PATCHES:
        /// Load detached parts (also their primary indexes) into the table.
        case ASTAlterCommand::ATTACH_PARTITION:
        /// Copy parts (`REPLACE_PARTITION` is also `ATTACH PARTITION ... FROM`) or run synchronously. The new
        /// `SELECT` of `MODIFY QUERY` is analyzed, which may read the sources it selects from, also through views.
        case ASTAlterCommand::MODIFY_QUERY:
        case ASTAlterCommand::MOVE_PARTITION:
        case ASTAlterCommand::REPLACE_PARTITION:
        case ASTAlterCommand::FETCH_PARTITION:
        case ASTAlterCommand::FREEZE_PARTITION:
        case ASTAlterCommand::FREEZE_ALL:
        case ASTAlterCommand::NO_TYPE:
        case ASTAlterCommand::EXECUTE_COMMAND:
            return false;
    }
}

bool isMetadataQuery(const IAST * ast)
{
    if (!ast || hasSubquery(*ast))
        return false;

    switch (ast->getQueryKind())
    {
        case IAST::QueryKind::Drop: /// Also DETACH and TRUNCATE
        case IAST::QueryKind::Rename:
        case IAST::QueryKind::Move: /// MOVE of access entities
        case IAST::QueryKind::Grant:
        case IAST::QueryKind::Revoke:
            return true;
        case IAST::QueryKind::System:
        {
            const auto * system = ast->as<ASTSystemQuery>();
            return system && isMetadataSystemQuery(*system);
        }
        case IAST::QueryKind::Create: /// Also ATTACH
        {
            /// Other kinds of objects (workloads, resources, access entities, functions, named
            /// collections, ...) have no data.
            const auto * create = ast->as<ASTCreateQuery>();
            if (!create)
                return true;
            /// A `SELECT` is analyzed to infer the structure (also for a view or `EMPTY AS SELECT`), which
            /// may read the sources it selects from, also through other views.
            bool has_select = create->select != nullptr;
            bool reads_table_function = create->as_table_function != nullptr;
            /// A table engine without the columns infers the structure from its data (e.g. `URL`, `File`, `S3`).
            bool has_columns = create->columns_list && create->columns_list->columns && !create->columns_list->columns->children.empty();
            bool is_table = create->table != nullptr; /// Not a database
            bool infers_structure = is_table && !create->isView() && !create->is_dictionary && create->storage
                && create->storage->engine && !has_columns && !create->select && create->as_table.empty();
            /// A dictionary may be loaded from its source by `CREATE`, also by a table with the `Dictionary`
            /// engine or one that takes its engine from another table (`AS other_table`), and a replicating
            /// engine (e.g. `MaterializedPostgreSQL`) copies the initial snapshot of its source.
            /// `ATTACH` loads the existing data of a table or a database (e.g. parts, or the saved rows of `Join` and `Set`).
            bool populates = create->attach || create->is_dictionary || !create->as_table.empty()
                || (create->storage && create->storage->engine
                    && (create->storage->engine->name == "Dictionary" || create->storage->engine->name.starts_with("Materialized")));
            return !has_select && !reads_table_function && !infers_structure && !populates && !create->is_populate && !create->is_clone_as
                && !create->has_attach_from_path;
        }
        case IAST::QueryKind::Alter:
        {
            const auto * alter = ast->as<ASTAlterQuery>();
            if (!alter)
                return true; /// Another kind of object (e.g. a named collection), which has no data
            if (!alter->command_list)
                return false;
            for (const auto & child : alter->command_list->children)
            {
                const auto * command = child->as<ASTAlterCommand>();
                if (!command || !isMetadataAlterCommand(*command))
                    return false;
            }
            return true;
        }
        /// Like `ATTACH`, loads the existing data of the table.
        case IAST::QueryKind::Undrop:
            return false;
        default:
            return false;
    }
}

ProcessList::EntryPtr ProcessList::insert(
    const String & query_,
    UInt64 normalized_query_hash,
    const IAST * ast,
    ContextMutablePtr query_context,
    UInt64 watch_start_nanoseconds,
    bool is_internal)
{
    EntryPtr res;

    const ClientInfo & client_info = query_context->getClientInfo();
    const Settings & settings = query_context->getSettingsRef();

    /// Read before `mutex` is taken: `Context::getUserID` locks the context, and `QueryStatus` is
    /// constructed with `mutex` held (see the constructor's own note about holding both locks).
    const std::optional<UUID> user_id = query_context->getUserID();

    if (client_info.current_query_id.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query id cannot be empty");

    bool is_unlimited_query = isUnlimitedQuery(ast) || is_internal || client_info.is_from_introspection_port;
    std::shared_ptr<QueryStatus> query;

    // Acquire a query slot and a memory reservation from the resource scheduler if necessary.
    // NOTE: There is a separate independent limit for the whole server `max_concurrent_queries`.
    // NOTE: If that limit is exhausted, the query will be later blocked and wait while holding a query slot.
    // NOTE: `MemoryReservation` admission may block here and evict other queries. The query may still be
    // rejected below by reject-fast `ProcessList` checks (`max_concurrent_queries*`, duplicate query id,
    // etc.), in which case the reservation was performed in vain. We accept this trade-off: the query slot
    // is already acquired at this point, and the upcoming multi-resource scheduler will admit query slots
    // and memory reservations together as a single allocation. Splitting their admission across the
    // `ProcessList` mutex would prevent that unification and is a worse design overall.
    QuerySlotPtr query_slot;
    MemoryReservationPtr memory_reservation;
    if (!is_unlimited_query)
    {
        // One deadline shared by the query slot and the memory reservation (acquired sequentially below),
        // so the whole pre-execution admission wait is bounded by a single `workload_admission_timeout_ms`
        // budget. `saturatedMilliseconds` caps the wait at ~1 year (the standard idiom — a longer timeout
        // is effectively no timeout); 0 is the explicit "no timeout" and maps to an infinite deadline.
        const UInt64 admission_timeout_ms = static_cast<UInt64>(settings[Setting::workload_admission_timeout_ms].totalMilliseconds());
        const auto admission_deadline = admission_timeout_ms
            ? std::chrono::steady_clock::now() + saturatedMilliseconds(admission_timeout_ms)
            : std::chrono::steady_clock::time_point::max();

        /// Hold a shared_ptr to keep the storage alive for the duration of this call, in case of concurrent shutdown.
        auto workload_entity_storage = query_context->getWorkloadEntityStoragePtr();
        String query_resource_name = workload_entity_storage->getQueryResourceName();
        if (!query_resource_name.empty())
        {
            if (ResourceLink link = query_context->getWorkloadClassifier()->get(query_resource_name))
                query_slot = std::make_unique<QuerySlot>(link, admission_deadline);
        }
        String memory_reservation_resource_name = workload_entity_storage->getMemoryReservationResourceName();
        if (!memory_reservation_resource_name.empty())
        {
            if (ResourceLink link = query_context->getWorkloadClassifier()->get(memory_reservation_resource_name))
            {
                // The link must point to a space-shared resource; fail loudly instead of dereferencing null.
                if (!link.allocation_queue)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Resource '{}' configured for memory reservation is not a `MEMORY RESERVATION` resource",
                        memory_reservation_resource_name);
                memory_reservation = std::make_unique<MemoryReservation>(
                    link, client_info.current_query_id, settings[Setting::reserve_memory], settings[Setting::min_bytes_to_spill], admission_deadline);
            }
        }
    }

    {
        LockAndOverCommitTrackerBlocker<std::unique_lock, Mutex> locker(mutex); /// To avoid deadlock in case of OOM
        auto & lock = locker.getUnderlyingLock();
        IAST::QueryKind query_kind = ast ? ast->getQueryKind() : IAST::QueryKind::Select;

        const auto queue_max_wait_ms = settings[Setting::queue_max_wait_ms].totalMilliseconds();
        if (!is_unlimited_query && max_size && non_internal_processes >= max_size)
        {
            if (queue_max_wait_ms)
                LOG_WARNING(getLogger("ProcessList"), "Too many simultaneous queries, will wait {} ms.", queue_max_wait_ms);
            if (!queue_max_wait_ms || !have_space.wait_for(lock, saturatedMilliseconds(queue_max_wait_ms),
                    [&]{ return non_internal_processes < max_size; }))
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous queries. Maximum: {}",
                                max_size);
        }

        if (!is_unlimited_query)
        {
            QueryAmount amount = getQueryKindAmount(query_kind);
            if (max_insert_queries_amount && query_kind == IAST::QueryKind::Insert && amount >= max_insert_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous insert queries. Maximum: {}, current: {}",
                                max_insert_queries_amount, amount);
            if (max_select_queries_amount && query_kind == IAST::QueryKind::Select && amount >= max_select_queries_amount)
                throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                                "Too many simultaneous select queries. Maximum: {}, current: {}",
                                max_select_queries_amount, amount);
        }

        {
            /**
             * `max_size` check above is controlled by `max_concurrent_queries` server setting and is a "hard" limit for how many
             * queries the server can process concurrently. It is configured at startup. When the server is overloaded with queries and the
             * hard limit is reached it is impossible to connect to the server to run queries for investigation.
             *
             * With `max_concurrent_queries_for_all_users` it is possible to configure an additional, runtime configurable, limit for query concurrency.
             * Usually it should be configured just once for `default_profile` which is inherited by all users. DBAs can override
             * this setting when connecting to ClickHouse, or it can be configured for a DBA profile to have a value greater than that of
             * the default profile (or 0 for unlimited).
             *
             * One example is to set `max_size=X`, `max_concurrent_queries_for_all_users=X-10` for default profile,
             * and `max_concurrent_queries_for_all_users=0` for DBAs or accounts that are vital for ClickHouse operations (like metrics
             * exporters).
             *
             * Another creative example is to configure `max_concurrent_queries_for_all_users=50` for "analyst" profiles running adhoc queries
             * and `max_concurrent_queries_for_all_users=100` for "customer facing" services. This way "analyst" queries will be rejected
             * once is already processing 50+ concurrent queries (including analysts or any other users).
             */

            if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_all_users]
                && non_internal_processes >= settings[Setting::max_concurrent_queries_for_all_users])
                throw Exception(
                    ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                    "Too many simultaneous queries for all users. "
                    "Current: {}, maximum: {}",
                    non_internal_processes,
                    settings[Setting::max_concurrent_queries_for_all_users].toString());
        }

        /** Why we use current user?
          * Because initial one is passed by client and credentials for it is not verified,
          *  and using initial_user for limits will be insecure.
          *
          * Why we use current_query_id?
          * Because we want to allow distributed queries that will run multiple secondary queries on same server,
          *  like SELECT count() FROM remote('127.0.0.{1,2}', system.numbers)
          *  so they must have different query_ids.
          */

        {
            auto user_process_list = user_to_queries.find(client_info.current_user);

            if (user_process_list != user_to_queries.end())
            {
                if (!is_unlimited_query && settings[Setting::max_concurrent_queries_for_user]
                    && user_process_list->second.non_internal_queries >= settings[Setting::max_concurrent_queries_for_user])
                    throw Exception(
                        ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                        "Too many simultaneous queries for user {}. "
                        "Current: {}, maximum: {}",
                        client_info.current_user,
                        user_process_list->second.non_internal_queries,
                        settings[Setting::max_concurrent_queries_for_user].toString());

                auto running_query = user_process_list->second.queries.find(client_info.current_query_id);

                if (running_query != user_process_list->second.queries.end())
                {
                    if (!settings[Setting::replace_running_query])
                        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Query with id = {} is already running.", client_info.current_query_id);

                    /// Ask queries to cancel. They will check this flag.
                    running_query->second->is_killed.store(true, std::memory_order_relaxed);

                    const auto replace_running_query_max_wait_ms = settings[Setting::replace_running_query_max_wait_ms].totalMilliseconds();
                    if (!replace_running_query_max_wait_ms || !have_space.wait_for(lock, saturatedMilliseconds(replace_running_query_max_wait_ms),
                        [&]
                        {
                            running_query = user_process_list->second.queries.find(client_info.current_query_id);
                            if (running_query == user_process_list->second.queries.end())
                                return true;
                            running_query->second->is_killed.store(true, std::memory_order_relaxed);
                            return false;
                        }))
                    {
                        throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                                        "Query with id = {} is already running and can't be stopped",
                                        client_info.current_query_id);
                    }
                }
            }
        }

        /// Check other users running query with our query_id
        if (auto query_user = queries_to_user.find(client_info.current_query_id); query_user != queries_to_user.end() && query_user->second != client_info.current_user)
        {
            throw Exception(ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
                            "Query with id = {} is already running by user {}",
                            client_info.current_query_id, query_user->second);
        }

        auto user_process_list_it = user_to_queries.find(client_info.current_user);
        if (user_process_list_it == user_to_queries.end())
        {
            user_process_list_it = user_to_queries.emplace(std::piecewise_construct,
                std::forward_as_tuple(client_info.current_user),
                std::forward_as_tuple(query_context->getGlobalContext(), this)).first;
        }
        ProcessListForUser & user_process_list = user_process_list_it->second;

        /// A new period starts with the user's next query rather than when its last one left, because a query
        /// settles what it holds after it leaves the list. Set the limits before the group is attached below, so
        /// that what the query already allocated is checked against them.
        const bool starts_a_new_period = user_process_list.queries.empty();
        if (starts_a_new_period)
            user_process_list.startNewPeriod();

        /// Track memory usage for all simultaneously running queries from single user.
        if (starts_a_new_period)
            user_process_list.user_memory_tracker.setHardLimit(settings[Setting::max_memory_usage_for_user]);
        else
            user_process_list.user_memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage_for_user]);
        user_process_list.user_memory_tracker.setSoftLimit(settings[Setting::memory_overcommit_ratio_denominator_for_user]);
        user_process_list.user_memory_tracker.setDescription("User");

        /// Actualize thread group info
        CurrentThread::attachQueryForLog(query_);
        auto thread_group = CurrentThread::getGroup();
        if (thread_group)
        {
            thread_group->performance_counters.setUserCounters(&user_process_list.user_performance_counters);

            /// A nested group (a view, a flush run by `SYSTEM FLUSH ASYNC INSERT QUEUE`, a dictionary load) is
            /// charged through the group above it, and would be charged to the user twice.
            if (!thread_group->isNested())
            {
                thread_group->memory_tracker.reparent(&user_process_list.user_memory_tracker);
                thread_group->memory_pressure_monitor.setParent(user_process_list.user_memory_pressure_monitor);
            }
            if (user_process_list.user_temp_data_on_disk)
            {
                TemporaryDataOnDiskSettings temporary_data_on_disk_settings
                {
                    .max_size_on_disk = settings[Setting::max_temporary_data_on_disk_size_for_query],
                    .compression_codec = settings[Setting::temporary_files_codec],
                    .buffer_size = settings[Setting::temporary_files_buffer_size],
                    .metrics = {}, /// Metrics are set by child scopes
                };

                if (user_process_list.user_temp_data_on_disk)
                    query_context->setTempDataOnDisk(std::make_shared<TemporaryDataOnDiskScope>(
                        user_process_list.user_temp_data_on_disk, std::move(temporary_data_on_disk_settings)));
            }

            /// Set query-level memory trackers
            thread_group->memory_tracker.setOrRaiseHardLimit(settings[Setting::max_memory_usage]);
            configureMemoryTrackerFromSettings(query_context->hasTraceCollector(), thread_group->memory_tracker, settings);

            /// Reapply sampling
            if (current_thread)
                current_thread->resolveMemorySampleConfig();

            if (query_context->hasTraceCollector() && settings[Setting::trace_profile_events])
            {
                const String & list_of_events_to_trace = settings[Setting::trace_profile_events_list];
                if (!list_of_events_to_trace.empty())
                {
                    /// Trace specific profile events
                    thread_group->performance_counters.setTraceProfileEvents(list_of_events_to_trace);
                }
                else
                {
                    /// Trace all profile events
                    thread_group->performance_counters.setTraceAllProfileEvents();
                }
            }

            thread_group->memory_tracker.setDescription("Query");
            thread_group->memory_tracker.setOvercommitWaitingTime(settings[Setting::memory_usage_overcommit_max_wait_microseconds]);

            /// NOTE: Do not set the limit for thread-level memory tracker since it could show unreal values
            ///  since allocation and deallocation could happen in different threads
        }

        query = std::make_shared<QueryStatus>(
            query_context,
            query_,
            normalized_query_hash,
            client_info,
            user_id,
            priorities.insert(
                settings[Setting::priority],
                saturatedMilliseconds(settings[Setting::low_priority_query_wait_time_ms].totalMilliseconds())),
            std::move(query_slot),
            std::move(memory_reservation),
            std::move(thread_group),
            query_kind,
            settings,
            watch_start_nanoseconds,
            is_internal);

        auto process_it = processes.emplace(
            processes.end(),
            query);

        /// We should not include internal queries for limiting the number of simultaneous queries.
        if (!is_internal)
        {
            ++non_internal_processes;
            increaseQueryKindAmount(query_kind);
        }

        bool registered_in_cancellation_checker = CancellationChecker::getInstance().appendTask(query, query_context->getSettingsRef()[Setting::max_execution_time].totalMicroseconds(), query_context->getSettingsRef()[Setting::timeout_overflow_mode]);

        res = std::make_shared<Entry>(*this, process_it, registered_in_cancellation_checker);

        (*process_it)->setUserProcessList(&user_process_list);
        (*process_it)->setProcessListEntry(res);

        user_process_list.queries.emplace(client_info.current_query_id, res->getQueryStatus());
        queries_to_user.emplace(client_info.current_query_id, client_info.current_user);
        if (!is_internal)
        {
            ++user_process_list.non_internal_queries;
        }

        if (!total_network_throttler && settings[Setting::max_network_bandwidth_for_all_users])
        {
            total_network_throttler = std::make_shared<Throttler>(
                settings[Setting::max_network_bandwidth_for_all_users],
                ProfileEvents::AllUsersThrottlerBytes,
                ProfileEvents::AllUsersThrottlerSleepMicroseconds);
        }

        if (!user_process_list.user_throttler)
        {
            if (settings[Setting::max_network_bandwidth_for_user])
                user_process_list.user_throttler = std::make_shared<Throttler>(
                    settings[Setting::max_network_bandwidth_for_user],
                    total_network_throttler,
                    ProfileEvents::UserThrottlerBytes,
                    ProfileEvents::UserThrottlerSleepMicroseconds);
            else if (settings[Setting::max_network_bandwidth_for_all_users])
                user_process_list.user_throttler = total_network_throttler;
        }
    }

    return res;
}


ProcessListEntry::~ProcessListEntry()
{
    if (registered_in_cancellation_checker)
    {
        /// We need to block the overcommit tracker here to avoid lock inversion because OvercommitTracker takes a lock on the ProcessList::mutex.
        /// When task is added, we lock the ProcessList::mutex, and then the CancellationChecker mutex.
        OvercommitTrackerBlockerInThread blocker;
        CancellationChecker::getInstance().appendDoneTasks(*it);
    }

    LockAndOverCommitTrackerBlocker<std::unique_lock, ProcessList::Mutex> lock(parent.getMutex());

    const String user = (*it)->getClientInfo().current_user;
    const String query_id = (*it)->getClientInfo().current_query_id;
    const IAST::QueryKind query_kind = (*it)->query_kind;
    const bool is_internal = (*it)->isInternal();

    const QueryStatusPtr process_list_element_ptr = *it;

    auto user_process_list_it = parent.user_to_queries.find(user);
    if (user_process_list_it == parent.user_to_queries.end())
    {
        LOG_ERROR(getLogger("ProcessList"), "Cannot find user in ProcessList");
        std::terminate();
    }

    ProcessListForUser & user_process_list = user_process_list_it->second;

    bool found = false;

    if (auto running_query = user_process_list.queries.find(query_id); running_query != user_process_list.queries.end())
    {
        if (running_query->second == process_list_element_ptr)
        {
            user_process_list.queries.erase(running_query->first);
            if (!is_internal)
            {
                --user_process_list.non_internal_queries;
            }
            found = true;
        }
    }

    /// Wait for the query if it is in the cancellation right now.
    parent.cancelled_cv.wait(lock.getUnderlyingLock(), [&]() { return process_list_element_ptr->is_cancelling == false; });

    if (auto query_user = parent.queries_to_user.find(query_id); query_user != parent.queries_to_user.end())
        parent.queries_to_user.erase(query_user);

    /// This removes the memory_tracker of one request.
    parent.processes.erase(it);

    if (!is_internal)
    {
        --parent.non_internal_processes;
        parent.decreaseQueryKindAmount(query_kind);
    }

    if (!found)
    {
        LOG_ERROR(getLogger("ProcessList"), "Cannot find query by query_id and pointer to ProcessListElement in ProcessListForUser");
        std::terminate();
    }

    parent.have_space.notify_all();

    /// The `user_to_queries` entry is intentionally kept (do not erase it here): `getUserInfo`
    /// reads entries lock-free via raw pointers and relies on them never being erased. Its trackers are reset
    /// when the user's next query arrives, see `ProcessList::insert`.
}


QueryStatus::QueryStatus(
    ContextPtr context_,
    const String & query_,
    UInt64 normalized_query_hash_,
    const ClientInfo & client_info_,
    const std::optional<UUID> & user_id_,
    QueryPriorities::Handle && priority_handle_,
    QuerySlotPtr && query_slot_,
    MemoryReservationPtr && memory_reservation_,
    ThreadGroupPtr && thread_group_,
    IAST::QueryKind query_kind_,
    const Settings & query_settings_,
    UInt64 watch_start_nanoseconds,
    bool is_internal_)
    : WithContext(context_)
    , query(query_)
    , normalized_query_hash(normalized_query_hash_)
    , client_info(client_info_)
    , user_id(user_id_)
    , query_slot(std::move(query_slot_))
    , memory_reservation(std::move(memory_reservation_))
    , thread_group(std::move(thread_group_))
    , watch(CLOCK_MONOTONIC, watch_start_nanoseconds, true)
    , priority_handle(std::move(priority_handle_))
    , global_overcommit_tracker(context_->getGlobalOvercommitTracker())
    , query_kind(query_kind_)
    , num_queries_increment(CurrentMetrics::Query)
    , is_internal(is_internal_)
    , is_ddl_or_on_cluster_internal(context_->isDDLOrOnClusterInternal())
{
    if (!is_internal)
        num_non_internal_queries_increment.emplace(CurrentMetrics::QueryNonInternal);

    /// We have to pass `query_settings_` to this constructor because we can't use `context_->getSettings().max_execution_time` here:
    /// a QueryStatus is created with `ProcessList::mutex` locked (see ProcessList::insert) and calling `context_->getSettings()`
    /// would lock the context's lock too, whereas holding two those locks simultaneously is not good.
    limits.max_execution_time = query_settings_[Setting::max_execution_time];
    overflow_mode = query_settings_[Setting::timeout_overflow_mode];
}

void QueryStatus::releaseWorkloadResources()
{
    releaseMemoryReservation();
    releaseQuerySlot();
}

void QueryStatus::releaseQuerySlot()
{
    query_slot.reset();
}

void QueryStatus::releaseMemoryReservation()
{
    MemoryReservationPtr delete_memory_reservation;
    {
        std::lock_guard lock(memory_reservation_mutex);
        delete_memory_reservation = std::move(memory_reservation);
    }
}

QueryStatus::~QueryStatus()
{
#if !defined(NDEBUG)
    /// Check that all executors were invalidated.
    for (const auto & [_, e] : executors)
        chassert(!e->executor);
#endif

    if (auto * memory_tracker = getMemoryTracker())
    {
        if (user_process_list)
            user_process_list->user_overcommit_tracker.onQueryStop(memory_tracker);
        if (global_overcommit_tracker)
            global_overcommit_tracker->onQueryStop(memory_tracker);
    }
}

void QueryStatus::ExecutorHolder::cancel()
{
    std::lock_guard lock(mutex);
    if (executor)
        executor->cancel(IProcessor::CancelReason::CancelledByUser);
}

void QueryStatus::ExecutorHolder::remove()
{
    std::lock_guard lock(mutex);
    executor = nullptr;
}

CancellationCode QueryStatus::cancelQuery(CancelReason reason, std::exception_ptr exception)
{
    {
        std::lock_guard<std::mutex> lock(cancel_mutex);

        if (is_killed)
            return CancellationCode::CancelSent;

        LOG_TRACE(getLogger("ProcessList"), "Cancelling the query (reason: {})", reason);

        is_killed = true;
        cancel_reason = reason;
        /// Only ever read (copied) after this, so it must not alias an object a caller still decorates.
        cancellation_exception = exception ? copyMutableException(exception) : nullptr;
    }

    std::vector<ExecutorHolderPtr> executors_snapshot;

    {
        /// Create a snapshot of executors under a mutex.
        std::lock_guard lock(executors_mutex);
        executors_snapshot.reserve(executors.size());
        for (const auto & [_, e] : executors)
            executors_snapshot.push_back(e);
    }

    /// We should call cancel() for each executor with unlocked executors_mutex, because
    /// cancel() can try to lock some internal mutex that is already locked by query executing
    /// thread, and query executing thread can call removePipelineExecutor and lock executors_mutex,
    /// which will lead to deadlock.
    /// Note that the size and the content of executors cannot be changed while
    /// executors_mutex is unlocked, because:
    /// 1) We don't allow adding new executors while cancelling query in addPipelineExecutor
    /// 2) We don't actually remove executor holder from executors in removePipelineExecutor,
    /// just mark that executor is invalid.
    /// So, it's ok to use a snapshot created above under a mutex, it won't be any differ from actual executors.
    for (const auto & e : executors_snapshot)
        e->cancel();

    return CancellationCode::CancelSent;
}

void QueryStatus::throwProperExceptionIfNeeded(const UInt64 & max_execution_time_us, const UInt64 & elapsed_ns)
{
    {
        std::lock_guard<std::mutex> lock(cancel_mutex);
        if (is_killed)
        {
            String additional_error_part;
            if (elapsed_ns)
                additional_error_part = fmt::format("elapsed {:.3f} ms, ", static_cast<double>(elapsed_ns) / 1000000ULL);

            if (cancel_reason == CancelReason::TIMEOUT)
                throw Exception(
                    ErrorCodes::TIMEOUT_EXCEEDED,
                    "Timeout exceeded: {}maximum: {:.3f} ms",
                    additional_error_part,
                    static_cast<double>(max_execution_time_us) / 1000);
            throwQueryWasCancelled();
        }
    }
}

void QueryStatus::addPipelineExecutor(IExecutor * e)
{
    /// In case of asynchronous distributed queries it is possible to call
    /// addPipelineExecutor() from the cancelQuery() context, and this will
    /// lead to deadlock.
    UInt64 max_exec_time = getContext()->getSettingsRef()[Setting::max_execution_time].totalMicroseconds();
    throwProperExceptionIfNeeded(max_exec_time, 0);

    std::lock_guard lock(executors_mutex);
    chassert(!executors.contains(e));
    executors[e] = std::make_shared<ExecutorHolder>(e);
}

void QueryStatus::removePipelineExecutor(IExecutor * e)
{
    ExecutorHolderPtr executor_holder;

    {
        std::lock_guard lock(executors_mutex);
        chassert(executors.contains(e));
        executor_holder = executors[e];
        executors.erase(e);
    }

    /// Invalidate executor pointer inside holder.
    /// We should do it with released executors_mutex to avoid possible lock order inversion.
    executor_holder->remove();
}

bool QueryStatus::checkTimeLimit()
{
    auto elapsed_ns = watch.elapsed();
    throwProperExceptionIfNeeded(limits.max_execution_time.totalMicroseconds(), elapsed_ns);

    return limits.checkTimeLimit(elapsed_ns, overflow_mode);
}

void QueryStatus::throwQueryWasCancelled() const
{
    /// A private copy per caller: `catch (Exception &)` handlers up the stack decorate what they catch.
    if (cancellation_exception)
        std::rethrow_exception(copyMutableException(cancellation_exception));
    else
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}

void QueryStatus::throwIfKilled()
{
    if (!is_killed.load())
        return;
    throwProperExceptionIfNeeded(limits.max_execution_time.totalMicroseconds(), 0);
}

CancelReason QueryStatus::getCancelReason() const
{
    std::lock_guard<std::mutex> lock(cancel_mutex);
    return cancel_reason;
}

bool QueryStatus::checkTimeLimitSoft()
{
    if (is_killed.load())
        return false;

    return limits.checkTimeLimit(watch.elapsedNanoseconds(), OverflowMode::BREAK);
}

void QueryStatus::setUserProcessList(ProcessListForUser * user_process_list_)
{
    user_process_list = user_process_list_;
}


void QueryStatus::setProcessListEntry(std::weak_ptr<ProcessListEntry> process_list_entry_)
{
    /// Synchronization is not required here because this function is only called from ProcessList::insert()
    /// when `ProcessList::mutex` is locked.
    if (!process_list_entry.expired() && !process_list_entry_.expired())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two entries in the process list cannot both use the same query status");
    process_list_entry = process_list_entry_;
}


std::shared_ptr<ProcessListEntry> QueryStatus::getProcessListEntry() const
{
    return process_list_entry.lock();
}


ThrottlerPtr QueryStatus::getUserNetworkThrottler()
{
    if (!user_process_list)
        return {};
    return user_process_list->user_throttler;
}

MemoryTracker * QueryStatus::getMemoryTracker() const
{
    if (!thread_group)
        return nullptr;
    return &thread_group->memory_tracker;
}

QueryStatusPtr ProcessList::tryGetProcessListElement(const String & current_query_id, const String & current_user)
{
    auto user_it = user_to_queries.find(current_user);
    if (user_it != user_to_queries.end())
    {
        const auto & user_queries = user_it->second.queries;
        auto query_it = user_queries.find(current_query_id);

        if (query_it != user_queries.end())
            return query_it->second;
    }

    return {};
}


std::optional<ProcessList::OwnQuery> ProcessList::tryGetOwnRunningQuery(const String & current_query_id, const UUID & user_id)
{
    LockAndBlocker lock(mutex);

    /// Not through `queries_to_user`: an entry there is erased by key alone, so it can be gone while a
    /// query that took the id over still runs. In `processes` an entry leaves only through its own
    /// iterator, and a query taking an id over is appended after the one it replaces, hence newest first.
    for (auto it = processes.rbegin(); it != processes.rend(); ++it)
    {
        const auto & elem = *it;
        if (elem->user_id != user_id || elem->getClientInfo().current_query_id != current_query_id)
            continue;

        /// `query` is set by the constructor and never mutated afterwards, so plain reads are safe.
        return OwnQuery{elem->getClientInfo().current_user, elem->query};
    }

    return {};
}


CancellationCode ProcessList::sendCancelToQueryImpl(
    const String & current_query_id, const String & current_user, const std::optional<UUID> & expected_user_id)
{
    QueryStatusPtr elem;

    /// Cancelling the query should be done without the lock.
    ///
    /// Since it may be not that trivial, for example in case of distributed
    /// queries it tries to cancel the query gracefully on shards and this can
    /// take a while, so acquiring a lock during this time will lead to wait
    /// all new queries for this cancellation.
    ///
    /// Another problem is that it can lead to a deadlock, because of
    /// OvercommitTracker.
    ///
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        LockAndBlocker lock(mutex);
        elem = tryGetProcessListElement(current_query_id, current_user);
        if (!elem || (expected_user_id && elem->user_id != *expected_user_id))
            return CancellationCode::NotFound;
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        Lock lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}


CancellationCode ProcessList::sendCancelToQuery(const String & current_query_id, const String & current_user)
{
    return sendCancelToQueryImpl(current_query_id, current_user, {});
}


CancellationCode ProcessList::sendCancelToQuery(
    const String & current_query_id, const String & current_user, const UUID & expected_user_id)
{
    return sendCancelToQueryImpl(current_query_id, current_user, expected_user_id);
}


CancellationCode ProcessList::sendCancelToQuery(QueryStatusPtr elem)
{
    /// Cancelling the query should be done without the lock.
    /// So here we first set is_cancelling, and later reset it.
    /// The ProcessListEntry cannot be destroy if is_cancelling is true.
    {
        LockAndBlocker lock(mutex);
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        Lock lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}


void ProcessList::registerPostgreSQLCancellationKey(Int32 connection_id, UInt32 secret_key, const String & query_id)
{
    LockAndBlocker lock(mutex);
    postgresql_cancellation_keys[{connection_id, secret_key}] = query_id;
}


void ProcessList::unregisterPostgreSQLCancellationKey(Int32 connection_id, UInt32 secret_key)
{
    LockAndBlocker lock(mutex);
    postgresql_cancellation_keys.erase({connection_id, secret_key});
}


CancellationCode ProcessList::sendCancelToPostgreSQLQuery(Int32 process_id, UInt32 secret_key)
{
    QueryStatusPtr elem;

    {
        LockAndBlocker lock(mutex);

        /// The request is unauthenticated, so a wrong secret must be indistinguishable from an
        /// unknown connection.
        auto cancellation_key = postgresql_cancellation_keys.find({process_id, secret_key});
        if (cancellation_key == postgresql_cancellation_keys.end())
            return CancellationCode::NotFound;

        const String & current_query_id = cancellation_key->second;
        auto query_user = queries_to_user.find(current_query_id);
        if (query_user == queries_to_user.end())
            return CancellationCode::NotFound;

        /// Other interfaces can forge this query-id shape, so only cancel PostgreSQL queries.
        elem = tryGetProcessListElement(current_query_id, query_user->second);
        if (!elem || elem->getClientInfo().interface != ClientInfo::Interface::POSTGRESQL)
            return CancellationCode::NotFound;

        /// Keep the verified entry by pointer to prevent query-id reuse races.
        elem->is_cancelling = true;
    }

    SCOPE_EXIT({
        DENY_ALLOCATIONS_IN_SCOPE;

        Lock lock(mutex);
        elem->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    return elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}


void ProcessList::killAllQueries(bool except_ddl_backups_and_restores)
{
    std::vector<QueryStatusPtr> cancelled_processes;

    SCOPE_EXIT({
        LockAndBlocker lock(mutex);
        for (auto & cancelled_process : cancelled_processes)
            cancelled_process->is_cancelling = false;
        cancelled_cv.notify_all();
    });

    {
        LockAndBlocker lock(mutex);
        cancelled_processes.reserve(processes.size());
        for (auto & process : processes)
        {
            if (except_ddl_backups_and_restores
                && (process->query_kind == IAST::QueryKind::Backup || process->query_kind == IAST::QueryKind::Restore)
                && process->is_ddl_or_on_cluster_internal)
                continue;

            cancelled_processes.push_back(process);
            process->is_cancelling = true;
        }
    }

    for (auto & cancelled_process : cancelled_processes)
        cancelled_process->cancelQuery(CancelReason::CANCELLED_BY_USER);

}

bool QueryStatus::updateProgressIn(const Progress & value)
{
    CurrentThread::updateProgressIn(value);
    progress_in.incrementPiecewiseAtomically(value);

    if (priority_handle)
        priority_handle->waitIfNeed();

    return !is_killed.load(std::memory_order_relaxed);
}

bool QueryStatus::updateProgressOut(const Progress & value)
{
    CurrentThread::updateProgressOut(value);
    progress_out.incrementPiecewiseAtomically(value);

    return !is_killed.load(std::memory_order_relaxed);
}


QueryStatusInfo QueryStatus::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    QueryStatusInfo res{};

    res.query             = query;
    res.normalized_query_hash = normalized_query_hash;
    res.query_kind        = query_kind;
    res.client_info       = client_info;
    res.elapsed_microseconds = watch.elapsedMicroseconds();
    res.is_cancelled      = is_killed.load(std::memory_order_relaxed);
    res.is_all_data_sent  = is_all_data_sent.load(std::memory_order_relaxed);
    res.is_internal       = is_internal;
    res.read_rows         = progress_in.read_rows;
    res.read_bytes        = progress_in.read_bytes;
    res.total_rows        = progress_in.total_rows_to_read;

    res.written_rows      = progress_out.written_rows;
    res.written_bytes     = progress_out.written_bytes;

    if (thread_group)
    {
        res.memory_usage = thread_group->memory_tracker.get();
        res.peak_memory_usage = thread_group->memory_tracker.getPeak();

        if (get_thread_list)
        {
            res.thread_ids = thread_group->getInvolvedThreadIds();
            res.peak_threads_usage = thread_group->getPeakThreadsUsage();
        }
        if (get_profile_events)
            res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(thread_group->performance_counters.getPartiallyAtomicSnapshot());
    }
    {
        std::lock_guard lock(memory_reservation_mutex);
        if (auto * reservation = memory_reservation.get())
            res.spillable_memory_bytes = reservation->getTotalReclaimable();
    }

    if (get_settings)
    {
        if (auto ctx = context.lock())
        {
            res.query_settings = std::make_shared<Settings>(ctx->getSettingsCopy());
            res.current_database = ctx->getCurrentDatabase();
        }
    }

    return res;
}


ProcessList::Info ProcessList::getInfo(bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    /// We have to copy `processes` first because `process->getInfo()` below can access the context to get the query settings,
    /// and it's better not to keep the process list's lock while doing that.
    std::vector<QueryStatusPtr> processes_copy;

    {
        LockAndBlocker lock(mutex);
        processes_copy.assign(processes.begin(), processes.end());
    }

    Info per_query_infos;
    per_query_infos.reserve(processes_copy.size());
    for (const auto & process : processes_copy)
        per_query_infos.emplace_back(process->getInfo(get_thread_list, get_profile_events, get_settings));

    return per_query_infos;
}

QueryStatusPtr ProcessList::getProcessListElement(const String & query_id) const
{
    LockAndBlocker lock(mutex);
    for (const auto & process : processes)
    {
        if (process->client_info.current_query_id == query_id)
            return process;
    }

    return nullptr;
}

QueryStatusInfoPtr ProcessList::getQueryInfo(const String & query_id, bool get_thread_list, bool get_profile_events, bool get_settings) const
{
    auto process = getProcessListElement(query_id);
    if (process)
        return std::make_shared<QueryStatusInfo>(process->getInfo(get_thread_list, get_profile_events, get_settings));

    return nullptr;
}

ProcessListForUser::ProcessListForUser(ProcessList * global_process_list)
    : ProcessListForUser(nullptr, global_process_list)
{}

ProcessListForUser::ProcessListForUser(ContextPtr global_context, ProcessList * global_process_list)
    : user_overcommit_tracker(global_process_list, this)
{
    user_memory_tracker.setOvercommitTracker(&user_overcommit_tracker);

    if (global_context)
    {
        const auto & settings = global_context->getSettingsRef();
        TemporaryDataOnDiskSettings temporary_data_on_disk_settings
        {
            .max_size_on_disk = settings[Setting::max_temporary_data_on_disk_size_for_user],
            .compression_codec = settings[Setting::temporary_files_codec],
            .buffer_size = settings[Setting::temporary_files_buffer_size],
            .metrics = {}, /// Metrics are set by child scopes
        };

        if (auto shared_temp_data = global_context->getSharedTempDataOnDisk())
            user_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(std::move(shared_temp_data),
                std::move(temporary_data_on_disk_settings));
    }
}


ProcessListForUserInfo ProcessListForUser::getInfo(bool get_profile_events) const
{
    ProcessListForUserInfo res;

    res.memory_usage = user_memory_tracker.get();
    res.peak_memory_usage = user_memory_tracker.getPeak();

    if (get_profile_events)
        res.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(user_performance_counters.getPartiallyAtomicSnapshot());

    return res;
}


ProcessList::UserInfo ProcessList::getUserInfo(bool get_profile_events) const
{
    /// Snapshot each user outside the lock (with sharded `User` counters `getInfo(true)` is
    /// O(num_events * cpus)). Safe: `user_to_queries` entries are never erased (see the comment in
    /// `ProcessListEntry`'s destructor) and `unordered_map` keeps element pointers valid across
    /// inserts, so they outlive the lock; `getInfo` reads only atomics.
    std::vector<std::pair<String, const ProcessListForUser *>> users;
    {
        LockAndBlocker lock(mutex);
        users.reserve(user_to_queries.size());
        for (const auto & [user, user_queries] : user_to_queries)
            users.emplace_back(user, &user_queries);
    }

    UserInfo per_user_infos;
    per_user_infos.reserve(users.size());
    for (const auto & [user, user_queries] : users)
        per_user_infos.emplace(user, user_queries->getInfo(get_profile_events));

    return per_user_infos;
}

void ProcessList::increaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        query_kind_amounts[query_kind] = 1;
    else
        found->second += 1;
}

void ProcessList::decreaseQueryKindAmount(const IAST::QueryKind & query_kind)
{
    auto found = query_kind_amounts.find(query_kind);
    /// TODO: we could just rebuild the map, as we have saved all query_kind.
    if (found == query_kind_amounts.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease before increase on '{}'", query_kind);
    if (found->second == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong query kind amount: decrease to negative on '{}', {}", query_kind, found->second);
    found->second -= 1;
}

ProcessList::QueryAmount ProcessList::getQueryKindAmount(const IAST::QueryKind & query_kind) const
{
    auto found = query_kind_amounts.find(query_kind);
    if (found == query_kind_amounts.end())
        return 0;
    return found->second;
}

void ProcessList::increaseWaitingQueryAmount()
{
    const UInt64 limit = max_waiting_queries_amount.load();
    UInt64 value = waiting_queries_amount.load();
    while (true)
    {
        if (limit && value >= limit)
            throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
                            "Too many simultaneous waiting queries. Maximum: {}, waiting: {}",
                            limit, value);
        if (waiting_queries_amount.compare_exchange_weak(value, value + 1))
            break;
    }

    /// WARNING: do not throw below this point, or the matching `decreaseWaitingQueryAmount` is never called.

    CurrentMetrics::add(CurrentMetrics::WaitingQuery);
}

void ProcessList::decreaseWaitingQueryAmount()
{
    UInt64 value = waiting_queries_amount.load();
    while (true)
    {
        if (value == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong waiting query amount: decrease to negative");
        if (waiting_queries_amount.compare_exchange_weak(value, value - 1))
            break;
    }

    CurrentMetrics::sub(CurrentMetrics::WaitingQuery);
}

void ProcessList::incrementWaiters(const QueryStatusPtr & status)
{
    std::lock_guard lock(status->waiting_mutex);
    if (status->waiting_threads == 0)
        increaseWaitingQueryAmount();
    ++status->waiting_threads;
}

void ProcessList::decrementWaiters(const QueryStatusPtr & status)
{
    std::lock_guard lock(status->waiting_mutex);
    if (status->waiting_threads == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong waiting thread amount: decrease to negative");

    if (--status->waiting_threads == 0)
        decreaseWaitingQueryAmount();
}

/// Process list and the query to account for, or nullptr when the waiter is not a counted query:
/// server or `clickhouse-local` startup, an AsyncLoader worker, or an internal query. A query's
/// process list element does not change, so one wait's increment and decrement always agree.
static ProcessList * getProcessListForWaitingQuery(QueryStatusPtr & status)
{
    auto query_context = CurrentThread::tryGetQueryContext();
    if (!query_context)
        return nullptr;
    status = query_context->getProcessListElementSafe();
    if (!status || status->isInternal())
        return nullptr;
    return &query_context->getGlobalContext()->getProcessList();
}

void onLoadJobWaitersIncrement(const LoadJobPtr &)
{
    QueryStatusPtr status;
    if (auto * process_list = getProcessListForWaitingQuery(status))
        process_list->incrementWaiters(status);
}

void onLoadJobWaitersDecrement(const LoadJobPtr &)
{
    QueryStatusPtr status;
    if (auto * process_list = getProcessListForWaitingQuery(status))
        process_list->decrementWaiters(status);
}

}
