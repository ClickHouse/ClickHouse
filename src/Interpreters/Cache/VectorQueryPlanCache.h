#pragma once

#include <Common/CacheBase.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <base/UUID.h>

#include <optional>
#include <chrono>
#include <limits>
#include <vector>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

class IAST;
class IDataType;
using ASTPtr = boost::intrusive_ptr<IAST>;
using DataTypePtr = std::shared_ptr<const IDataType>;
struct Settings;

// /// Does AST contain non-deterministic functions like rand() and now()?
// bool astContainsNonDeterministicFunctions(ASTPtr ast, ContextPtr context);

// /// Does AST contain system tables like "system.processes"?
// bool astContainsSystemTables(ASTPtr ast, ContextPtr context);

/// VectorQueryPlanCache stores reusable AST and QueryPlan snapshots for normalized SELECT statements.
///
/// The cache serves two related reuse modes:
/// 1. AST reuse: skip SQL parsing by restoring literals into a cached AST clone.
/// 2. QueryPlan reuse: skip planning by restoring literals into a cached QueryPlan clone.
///
/// Every entry is keyed by the normalized query text plus the execution context that can
/// affect planning semantics, such as the current database, settings, user identity, and
/// plan-cache feature switches. The cache is intentionally best-effort rather than fully
/// transactional: entries expire via TTL, and table changes invalidate matching entries
/// through a best-effort table-to-key reverse index maintained inside this class.
class VectorQueryPlanCache
{
public:
    struct ASTLiteralPosition
    {
        std::vector<UInt32> path;
        UInt32 parameter_index = std::numeric_limits<UInt32>::max();
        Int32 field_type = -1;
        DataTypePtr target_type;
        std::vector<String> function_list;
        String ast_path_name;
        String identifier_name;
        Int32 step_type = -1;
    };

    /// PlanConstantBinding identifies one mutable constant slot inside a cached QueryPlan.
    /// The binding tells QueryParameterizer how to reach the owning plan node, which
    /// ActionsDAG or QueryInfo payload to patch, and which runtime parameter index
    /// should be written back on a cache hit.
    struct PlanConstantBinding
    {
        std::vector<UInt32> plan_node_path;
        UInt32 parameter_index = 0;
        String dag_scope;
        UInt32 dag_node_index = 0;
        String value_text;
        Int32 field_type = -1;
        /// Precise target type collected while the original plan is still fresh.
        /// Vector-query restore uses this to parse runtime array literals exactly
        /// once and then reuse the typed Field for both AST and plan replacement.
        DataTypePtr target_type;
    };

    /// Key is the stable identity of one reusable SELECT shape.
    ///
    /// Two executions may share the same cache entry only if they produce the same
    /// normalized SQL and run under the same planning-relevant environment. The key
    /// therefore excludes purely execution-time details but includes anything that may
    /// change table resolution, access rules, or generated plan structure.
    struct Key
    {
        const String query_string;              /// Normalized query string used as a stable key component (SETTINGS stripped).
        String current_database;                /// Current database to disambiguate unqualified identifiers.
        std::vector<std::pair<String, String>> relevant_settings; /// Deterministic (name, value) list of settings affecting the plan. Sorted for deterministic keys.

        const Block header;                      /// Output header (used only for query-result cache parity).
        String header_structure;                 /// Cached header structure string for cheap comparison (avoids repeated dumpStructure calls).
        std::optional<UUID> user_id;             /// User identity for security-sensitive queries.
        std::vector<UUID> current_user_roles;    /// Roles also affect access policies and therefore plan correctness. Sorted in constructor for deterministic keys.
        bool is_shared;                          /// Whether the cache entry is shared across users or private to a user.
        std::chrono::time_point<std::chrono::system_clock> expires_at; /// Per-entry TTL; stale entries are ignored.
        const String tag;                        /// Tag for selective invalidation (SYSTEM DROP QUERY PLAN CACHE TAG).
        /// Are the chunks in the entry compressed?
        /// (we could theoretically apply compression also to the totals and extremes but it's an obscure use case)
        const bool is_compressed;

        Key(String query_string_,
            const String & current_database_,
            const Settings & settings,
            Block header_,
            std::optional<UUID> user_id_,
            const std::vector<UUID> & current_user_roles_,
            bool is_shared_,
            std::chrono::time_point<std::chrono::system_clock> expires_at_,
            bool is_compressed);

        Key(String query_string_,
            const String & current_database_,
            const Settings & settings,
            std::optional<UUID> user_id_,
            const std::vector<UUID> & current_user_roles_);

        bool operator==(const Key & other) const;
    };

    /// Entry stores all reusable artifacts for one normalized SELECT shape.
    ///
    /// Some modes write only the AST, some write both AST and QueryPlan, and future
    /// writes may merge missing components into an existing entry. `table_names`
    /// records the resolved source tables so mutating statements can invalidate only
    /// the affected cache entries instead of dropping the entire cache.
    struct Entry
    {
        std::shared_ptr<QueryPlan> plan_cache;  /// Cloned query plan stored for reuse.
        size_t plan_size = 0;                    /// Estimated plan size to charge cache usage.
        ASTPtr ast;                              /// Cloned AST snapshot stored for reuse.
        size_t ast_size = 0;                     /// Serialized AST size estimate for cache accounting.
        std::vector<ASTLiteralPosition> ast_literal_positions; /// Ordered ASTLiteral node positions for fast replacement.
        std::vector<PlanConstantBinding> plan_constant_bindings; /// QueryPlan constant-node positions for fast replacement.
        std::vector<String> table_names;         /// Canonical `db.table` names referenced by this cached SELECT entry.
    };

private:
    struct KeyHasher
    {
        size_t operator()(const Key & key) const;
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };

    struct IsStale
    {
        bool operator()(const Key & key) const;
    };

    using Cache = CacheBase<Key, Entry, KeyHasher, EntryWeight>;

public:
    class Writer
    {
    public:
        Writer(const Writer & other);

        /// Store a cloned QueryPlan snapshot into the entry currently being built.
        /// The writer owns the clone so the caller can keep mutating its original plan.
        void setPlan(QueryPlan & plan);
        /// Store a cloned AST snapshot so AST-cache hits can skip SQL parsing.
        void setAst(const ASTPtr & ast);
        /// Store the ordered AST literal map used during AST constant restoration.
        void setAstLiteralPositions(std::vector<ASTLiteralPosition> ast_literal_positions);
        /// Store the ordered QueryPlan constant map used during plan constant restoration.
        void setPlanConstantBindings(std::vector<PlanConstantBinding> plan_constant_bindings);
        /// Store the canonical table list used to invalidate cached plans when a table changes.
        void setTableNames(std::vector<String> table_names);
        /// Publish the finished entry if it satisfies admission checks such as build
        /// time, entry size, and cache-capacity quotas. Publication is atomic from
        /// the caller's perspective: the entry becomes visible only after all stored
        /// components and table mappings have been finalized.
        void finalizeWrite();

    private:
        std::mutex mutex;
        VectorQueryPlanCache & owner; /// Owning cache, used to rebuild reverse table mappings after publication.
        Cache & cache;          /// Underlying LRU/TTL container shared by all readers and writers.
        const Key key;          /// Immutable destination key for the entry being assembled.
        const size_t max_entry_size_in_bytes;
        const size_t max_entry_size_in_rows;
        const std::chrono::time_point<std::chrono::system_clock> query_start_time;
        const std::chrono::milliseconds min_plan_build_time;
        Cache::MappedPtr query_plan TSA_GUARDED_BY(mutex) = std::make_shared<VectorQueryPlanCache::Entry>(); /// Entry under construction.
        std::atomic<bool> skip_insert = false; /// Future guard for admission short-circuiting.
        std::atomic<bool> was_finalized = false; /// Prevents publishing the same writer twice. Atomic for thread-safe copy.
        LoggerPtr logger = getLogger("VectorQueryPlanCacheWriter");

        Writer(
            VectorQueryPlanCache & owner_,
            Cache & cache_,
            const Key & key_,
            size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_,
            std::chrono::milliseconds min_plan_build_time_);

        friend class VectorQueryPlanCache;
    };

    class Reader
    {
    public:
        /// Cheap existence checks used before attempting to clone cached objects.
        /// These calls avoid cloning large AST / QueryPlan objects when the key is absent.
        bool hasCacheEntryForKey(bool update_profile_events = true) const;
        bool hasAstCacheEntryForKey(bool update_profile_events = true) const;

        /// Return a cloned QueryPlan snapshot. The caller receives ownership of the clone
        /// and can safely patch constants without mutating the cache entry itself.
        QueryPlanPtr getPlan();
        /// Return a cloned cached AST snapshot for this key.
        ASTPtr getAst();
        /// Return the AST literal map associated with the cache entry.
        const std::vector<ASTLiteralPosition> & getAstLiteralPositions() const;
        /// Return the QueryPlan constant map associated with the cache entry.
        const std::vector<PlanConstantBinding> & getPlanConstantBindings() const;

    private:
        /// The reader snapshots the shared entry under VectorQueryPlanCache::mutex and then
        /// releases that mutex immediately. After construction, clones are produced from
        /// the reader-local shared_ptr snapshots, not from the cache map itself.
        Reader(Cache & cache_, const Key & key, const std::lock_guard<std::mutex> &);

        std::shared_ptr<QueryPlan> plan; /// Snapshot of plan entry for this reader instance.
        ASTPtr ast;                      /// Snapshot of AST entry for this reader instance.
        std::vector<ASTLiteralPosition> ast_literal_positions;
        std::vector<PlanConstantBinding> plan_constant_bindings;
        Cache::MappedPtr entry_mapped;   /// Underlying cache entry for AnalysisResult reuse.
        size_t plan_size = 0;            /// Size of the plan for deferred statistics counting.
        size_t ast_size = 0;              /// Size of the AST for deferred statistics counting.
        LoggerPtr logger = getLogger("VectorQueryPlanCacheReader");

        friend class VectorQueryPlanCache;
    };

    VectorQueryPlanCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    /// Update the cache-wide capacity limits and per-entry admission thresholds.
    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    Reader createReader(const Key & key);
    Writer createWriter(
        const Key & key,
        std::chrono::milliseconds min_plan_build_time,
        size_t max_query_plan_cache_size_in_bytes_quota,
        size_t max_query_cache_entries_quota);

    void clear(const std::optional<String> & tag);
    /// Remove every entry whose cached SELECT depends on one of the provided tables.
    /// Table names must be canonical `db.table` strings.
    void clearByTables(const std::vector<String> & table_names);
    /// Remove every entry that depends on any table inside one of the provided databases.
    /// This is used for broad DDL such as DROP/RENAME/ALTER DATABASE or TRUNCATE ALL TABLES.
    void clearByDatabases(const std::vector<String> & database_names);
    /// Inspect a mutating query AST and invalidate every cached SELECT that depends on
    /// the affected table(s) or database(s) before the mutation executes.
    void invalidateByQuery(const ASTPtr & ast, ContextPtr context);

    /// Extract canonical `db.table` names from a SELECT AST so cache entries can be
    /// invalidated later when any referenced source table changes.
    static std::vector<String> collectTableNames(const ASTPtr & ast, ContextPtr context);

    /// Cache restore currently depends on a stable literal order. Queries with
    /// subqueries may reorder constants during optimization, so cache writes for
    /// those statements are skipped.
    static bool containsSubquery(const ASTPtr & ast);

private:
    /// Reverse lookup from canonical `db.table` name to every cache key whose entry
    /// currently depends on that table. The map is rebuilt after publication/removal
    /// instead of being updated incrementally in many scattered call sites, which keeps
    /// executeQuery and the interpreters simpler.
    using TableKeys = std::map<String, std::unordered_set<Key, KeyHasher>>;

    /// Rebuild the reverse table-to-key index from the current cache contents.
    /// Callers must hold `mutex` because `table_to_keys` is guarded by that mutex.
    void rebuildTableMappings() TSA_REQUIRES(mutex);

    std::shared_ptr<Cache> cache;       /// Underlying cache storage with LRU + TTL policy.
    mutable std::mutex mutex;

    TableKeys table_to_keys TSA_GUARDED_BY(mutex); /// Reverse invalidation index for per-table cache drops.

    size_t max_entry_size_in_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t max_entry_size_in_rows TSA_GUARDED_BY(mutex) = 0;

    friend class StorageSystemQueryCache;
};

using VectorQueryPlanCachePtr = std::shared_ptr<VectorQueryPlanCache>;
using VectorQueryPlanCacheWriter = VectorQueryPlanCache::Writer;
using VectorQueryPlanCacheReader = VectorQueryPlanCache::Reader;

}
