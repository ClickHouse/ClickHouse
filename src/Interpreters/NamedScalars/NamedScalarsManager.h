#pragma once

#include <Interpreters/NamedScalars/INamedScalarDefinitionStore.h>
#include <Interpreters/NamedScalars/NamedScalar.h>

#include <optional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

class NamedScalarValueBackendLocal;
class SharedNamedScalarsWatcher;

/// CREATE request at the interpreter/manager boundary. The interpreter has
/// validated permissions, materialized UUID/cache kind/definer into SQL,
/// and formatted the CREATE query that the definition store will persist.
struct NamedScalarCreateRequest
{
    NamedScalarCacheKind cache_kind = NamedScalarCacheKind::Local;
    String name;
    String formatted_create_query;
    bool if_not_exists = false;
    bool or_replace = false;
};

struct NamedScalarWithScope
{
    NamedScalarCacheKind cache_kind = NamedScalarCacheKind::Local;
    NamedScalarPtr scalar;
};

/// Public facade for the named-scalars feature. The DDL interpreter,
/// `getNamedScalar*` functions, and `system.named_scalars` go through
/// here.
///
/// Module overview
/// ---------------
/// The user-visible namespace is singular. `LOCAL` / `SHARED` is the
/// value-cache kind of a definition, not a second name scope.
///
///     NamedScalarsManager
///         |
///         +-- active definition store
///         |       +-- NamedScalarDefinitionStoreLocal  (disk definitions)
///         |       \-- NamedScalarDefinitionStoreShared (Keeper definitions + watches)
///         |
///         +-- value backends selected per scalar
///         |       +-- NamedScalarValueBackendLocal     (disk value cache)
///         |       \-- NamedScalarValueBackendShared    (Keeper value cache)
///         |
///         \-- named scalar map
///                 \_ NamedScalar (definition, current value, refresh task, fences)
///
/// `NamedScalar` owns the per-name lifecycle: an immutable parsed
/// definition, the current cached value, refresh-runtime state, a
/// single `BackgroundSchedulePool` task, a `reload_requested` flag set
/// by backend value-watch fires, and a `live` flag. A separate
/// raw-byte value backend is what a scalar calls to read / watch /
/// publish durable values.
///
/// Definition lifecycle: definitions are immutable. CREATE OR REPLACE
/// produces a NEW NamedScalar with a NEW UUID; this manager swaps it
/// into the catalog map and shuts the old one down. There is no
/// in-place reconcile path on `NamedScalar`.
///
/// Concurrency invariants (live in `NamedScalar`, surfaced here for context):
///   * Readers (getNamedScalar, system.named_scalars) copy stable
///     snapshots and release scalar locks immediately.
///   * Publish path - initial evaluate, refresh body, backend-triggered
///     value reload - serialises on the per-scalar `publish_mutex`.
///   * `NamedScalar::shutdown()` is the publish-vs-DROP fence: it blocks
///     until any in-flight publish returns, so the caller's subsequent
///     durable removal can't be overtaken by a stale publish.
///
/// Shutdown is explicit: the shared watcher is stopped first, then scalar
/// refresh tasks are drained while Context services, Keeper, and schedule
/// pools are still alive. Destructors are idempotent fallbacks only.
class NamedScalarsManager
{
public:
    static void checkName(const String & name);

    NamedScalarsManager(const String & definitions_disk_path,
                        const String & definitions_zookeeper_path,
                        const String & local_cache_path,
                        NamedScalarCacheKind default_cache_kind_,
                        const ContextPtr & global_context);
    ~NamedScalarsManager();

    NamedScalarsManager(const NamedScalarsManager &) = delete;
    NamedScalarsManager & operator=(const NamedScalarsManager &) = delete;

    void initialize(const ContextPtr & global_context);
    void shutdown();

    /// Hot path for the getNamedScalar UDF and other readers that
    /// don't care about the cache kind. Returns null if no scalar with
    /// this name. Use tryGetScopedScalar when cache_kind is needed.
    NamedScalarPtr tryGetScalar(const String & name) const;
    std::optional<NamedScalarWithScope> tryGetScopedScalar(const String & name) const;
    std::vector<NamedScalarWithScope> listScalars() const;

    /// Authoritative cache-kind lookup: prefers the local map (cheap),
    /// falls back to the persisted definition (Keeper RTT for SHARED).
    /// Returns nullopt if no definition exists for the name. Used by
    /// CREATE OR REPLACE kind-change guard and DROP ON CLUSTER routing
    /// — both want the persisted truth, not the live-map view.
    std::optional<NamedScalarCacheKind> getCacheKind(
        const String & name,
        const ContextPtr & context,
        LoggerPtr log) const;
    NamedScalarCacheKind getDefaultCacheKind() const { return default_cache_kind; }

    /// Throws SHARED_NAMED_SCALARS_NOT_CONFIGURED when the server uses
    /// disk definitions. A shared value cache needs a Keeper-backed
    /// definition store so DROP / OR REPLACE have one cluster-wide owner.
    void ensureCreatable(NamedScalarCacheKind cache_kind) const;

    /// Authoritative existence check; for shared scope queries Keeper
    /// directly so an IF NOT EXISTS short-circuit can't miss entries the
    /// watcher hasn't reconciled yet.
    bool definitionExists(const String & name) const;

    bool create(NamedScalarCreateRequest request, const ContextPtr & context);
    bool drop(const String & name, bool throw_if_not_exists);
    void refreshNow(const String & name);
    /// Per-server pause; not propagated to peers, not persisted.
    void setRefreshPaused(const String & name, bool paused);
    void setAllRefreshesPaused(bool paused);

private:
    friend class SharedNamedScalarsWatcher;

    bool usesKeeperDefinitions() const { return definition_store->isKeeperBacked(); }
    INamedScalarValueBackend & valueBackendFor(NamedScalarCacheKind cache_kind) const;
    NamedScalarCacheKind cacheKindFromDefinitionBlob(const String & definition_blob, const ContextPtr & context, LoggerPtr log) const;

    void installScalar(NamedScalarPtr scalar, NamedScalarCacheKind cache_kind);

    /// Like `installScalar` but returns the replaced predecessor (or null
    /// if no entry existed) WITHOUT shutting it down. The caller is
    /// responsible for calling `shutdown()` on the returned scalar AFTER
    /// any catalog mutex is released. Predecessor `shutdown()` cancels
    /// and joins the in-flight refresh body; doing that under
    /// `create_drop_mutex` would serialise all named-scalar DDL on the
    /// slowest predecessor. Use this when adding
    /// the new scalar inside a catalog lock and dropping the predecessor
    /// outside it.
    [[nodiscard]] NamedScalarPtr swapScalar(NamedScalarPtr scalar, NamedScalarCacheKind cache_kind);
    bool dropScalar(const String & name);
    void shutdownScalars();
    std::vector<NamedScalarPtr> listAllScalars() const;
    void installStoredDefinition(
        const ContextPtr & context,
        const String & name,
        const String & definition_blob,
        LoggerPtr log);

    NamedScalarDefinitionStorePtr definition_store;
    WatchableNamedScalarDefinitionStorePtr watchable_definition_store;
    NamedScalarCacheKind default_cache_kind = NamedScalarCacheKind::Local;
    std::unique_ptr<NamedScalarValueBackendLocal> local_value_backend;
    std::unique_ptr<INamedScalarValueBackend> shared_value_backend;
    std::unique_ptr<SharedNamedScalarsWatcher> watcher;

    mutable std::shared_mutex scalars_mutex;
    std::unordered_map<String, NamedScalarWithScope> scalars;
    std::mutex create_drop_mutex;
};

}
