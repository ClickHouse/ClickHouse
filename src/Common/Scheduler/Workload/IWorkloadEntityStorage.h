#pragma once

#include <base/types.h>
#include <base/scope_guard.h>

#include <limits>
#include <string_view>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

class IAST;
struct Settings;
class BackupEntriesCollector;
class RestorerFromBackup;

/*
 * === WORKLOAD ENTITY STORAGE ===
 *
 * Entity. A WORKLOAD or a RESOURCE, identified by its name and represented by its normalized CREATE query
 * (`CREATE WORKLOAD ...` or `CREATE RESOURCE ...` without `OR REPLACE` / `IF NOT EXISTS`). Names are shared
 * by both types: a workload and a resource cannot have the same name in one view.
 *
 * Storage layer and chain. Storages are linked into a chain through `next_storage`: every layer wraps the
 * next (inner) one and the inner layer is unaware of the outer one. The server chain, from the outermost
 * to the innermost layer, is:
 *   implicit (`WorkloadEntityImplicitStorage`)
 *     -> SQL, persisted (`WorkloadEntityKeeperStorage` or `WorkloadEntityDiskStorage`)
 *       -> configuration (`WorkloadEntityConfigStorage`)
 * Readers (the resource manager, disks, system tables, interpreters) work with the outermost layer.
 *
 * Own, inherited and visible entities. Every layer distinguishes:
 *  - own entities: entities stored in this layer (written by SQL, loaded from configuration, Keeper or
 *    disk, or synthesized by the implicit layer);
 *  - inherited entities: the visible entities of the inner layer;
 *  - visible entities: the merged view this layer exposes to its readers and subscribers.
 *
 * Priority. The inner layer has higher priority than the outer one: configuration > SQL > implicit.
 *
 * Claim key. Every entity claims a set of keys: its name and, for a resource, one key per access-mode role
 * it takes. The roles are MASTER THREAD, WORKER THREAD, QUERY and MEMORY RESERVATION: each of them is served
 * by at most one resource (see `getMasterThreadResourceName` and friends). READ DISK and WRITE DISK claim
 * nothing: several resources may serve the same disk.
 *
 * Shadowing. An own entity is shadowed when any of its claim keys is also claimed by an inherited entity of
 * the same kind: a resource by a resource claiming its name or a role, a workload by a workload of its name.
 * A shadowed entity stays stored in its layer but is not visible; it becomes visible again when the
 * inherited entity goes away. Shadowing always hides the whole entity, never a part of it. The children of
 * a workload shadowed by an inherited workload of the same name attach to the inherited workload. A workload
 * whose parent is not a visible workload (e.g. the parent is hidden) is hidden as well, transitively, so a
 * visible workload always has a visible parent. The visible entities are the inherited entities plus the own
 * entities that are neither shadowed nor hidden. `FOR` clauses do not hide entities: a visible workload may
 * name a hidden resource in its `FOR` clause, and such settings apply to no resource.
 *
 * Conflict. Two own entities of one layer that share a claim key, an own entity and an inherited entity of
 * another kind with the same name, or a SQL statement whose entity shares a claim key with an inherited
 * entity. `storeEntity` rejects conflicts with an exception, checking the new entity against all own entities
 * (hidden ones included) and inherited entities. Loading the configuration rejects conflicts between its
 * entities with an exception, so a bad configuration reload is not applied. Persisted layers (Keeper, disk)
 * may hold conflicting entities written by older versions, so loading them never fails because of a
 * conflict: among conflicting own entities the one with the lexicographically smaller name wins and the
 * others are hidden, with an error in the log naming them. An own entity that conflicts with an inherited
 * entity of another kind (after a load of either layer) is hidden, with an error in the log naming both
 * entities and their layers. Any other overlap of a loaded entity with inherited entities is not a conflict:
 * the loaded entity is shadowed. Hence the visible entities of a layer never conflict.
 *
 * Implicit entities. Entities synthesized by the implicit layer from the server configuration (see
 * `ImplicitWorkloadEntitiesConfig`): the resources `__server_cpu__` (MASTER THREAD, WORKER THREAD) and
 * `__server_memory__` (MEMORY RESERVATION) and the workload `default`. They are own entities of the
 * implicit layer, so any user entity claiming the same name or role shadows them, or hides them by a
 * conflict if it is of another kind (e.g. a user resource `default`). The names of the implicit resources
 * are reserved: SQL statements and the configuration reject user entities with these names. A persisted
 * layer (Keeper, disk) may hold such an entity written by an older version: it is loaded hidden, with an
 * error in the log, and can be dropped.
 *
 * Root workload. The workload entity with the empty name `""`. It is the parent of every workload without
 * an explicit parent, and its settings are the per-resource root limits (e.g. `max_concurrent_threads =
 * 8 FOR __server_cpu__`). It has no SQL text form, since an empty identifier cannot be parsed. Only the
 * implicit layer provides it, deriving it from its visible entities; user entities with the empty name are
 * rejected. The resource manager creates no workload node for it and applies its settings to the root node
 * of every resource.
 *
 * Events and transactions. Subscribers of a layer receive changes of its visible entities only. Every
 * change in any layer (a SQL statement, a load or reload, a change of the implicit entities) is turned by
 * every outer layer into exactly one transaction: a vector of events that turns the old visible entities
 * into the new ones. Events are ordered so that every intermediate state is consistent: an entity is
 * created before entities referencing it and dropped after them. An unchanged view produces no transaction.
 * Transactions of a layer, including the initial snapshot passed to a new subscriber, are delivered one at
 * a time in the order they were produced (see `notify_mutex` in WorkloadEntityStorageBase.h). Handlers are
 * called without the layer's state lock, except on the Keeper refresh paths (the watching thread, loading,
 * and the retry of `storeEntity`), where `WorkloadEntityKeeperStorage` holds its lock. Hence a handler must
 * not call back into the layer that notifies it.
 *
 * References. A workload references its parent workload and the resources named in its `FOR` clauses.
 * A name resolves to the inherited entity first, as the user sees it, then to the own entity; an own entity
 * shadowed by name is of the same kind, so the kind of the target does not depend on shadowing. A layer
 * validates references of a new entity against its own and inherited entities, so it cannot reference
 * entities of an outer layer: persisted SQL entities cannot reference implicit entities (an implicit
 * `default` cannot have children and a `FOR` clause cannot name an implicit resource), and the
 * configuration references only its own entities. Hence a parent cycle cannot span several layers.
 * `storeEntity` rejects an entity of another kind than an existing reference to its name expects (e.g. a
 * resource named as a parent), and `removeEntity` rejects dropping an entity referenced by a visible or own
 * entity. The cycle check of `storeEntity` walks the references of both the own and the inherited entity of
 * every name, not only of visible ones, so a hidden entity cannot hide a cycle that appears once it is
 * visible. Loading the configuration rejects the whole reload, keeping the previous entities, if a parent or
 * a `FOR` target is missing in the configuration or is of the wrong kind, or if its workloads form a parent
 * cycle. Loading persisted data (Keeper, disk) never fails because of references: a workload with a missing
 * or wrong-kind parent is hidden, a `FOR` clause naming no resource applies to no resource, and every
 * workload on a parent cycle is hidden with an error in the log.
 *
 * Writes, backup and restore. `storeEntity`, `removeEntity`, `backup` and `restore` are served by the SQL
 * layer: the implicit layer forwards them, and the configuration is read-only. Implicit entities cannot be
 * dropped, even while shadowed: they are defined by the server settings. A backup contains only the own
 * entities of the SQL layer, hidden ones included: configuration entities belong to the server
 * configuration, and implicit entities are synthesized from it. A restore recreates the entities of a backup
 * in the SQL layer through `storeEntity`, as SQL statements, so a backed up entity that a SQL statement
 * rejects at restore time (e.g. a hidden one) fails the restore.
 *
 * Source. `getEntitySource` returns the name of the layer that owns a visible entity, e.g. `implicit`,
 * `keeper`, `disk` or `configuration`. It is shown in the `source` column of `system.workloads` and
 * `system.resources`.
 */

enum class WorkloadEntityType : uint8_t
{
    Workload,
    Resource,

    MAX
};

/// Reserved names of the implicit resources.
inline constexpr std::string_view IMPLICIT_CPU_RESOURCE_NAME = "__server_cpu__";
inline constexpr std::string_view IMPLICIT_MEMORY_RESOURCE_NAME = "__server_memory__";

/// Server configuration of the implicit entities (see above). Limits are already resolved from the
/// server settings to effective numbers; a limit that is off or unbounded is `unlimited`.
struct ImplicitWorkloadEntitiesConfig
{
    /// Same value as `WorkloadSettings::unlimited`.
    static constexpr Int64 unlimited = std::numeric_limits<Int64>::max();

    /// Synthesizes the resource `__server_cpu__` and limits the root workload by `cpu_slots`.
    bool respect_cpu_limit = false;

    /// Synthesizes the resource `__server_memory__` and limits the root workload by `memory_bytes`.
    bool respect_memory_limit = false;

    /// Synthesizes the workload `default`.
    bool implicit_default_workload = false;

    /// Number of concurrent threads for the single resource holding both MASTER THREAD and WORKER THREAD.
    Int64 cpu_slots = unlimited;

    /// Memory reservation budget in bytes for the resource holding MEMORY RESERVATION.
    Int64 memory_bytes = unlimited;
};

/// Interface for a storage of workload entities (WORKLOAD and RESOURCE).
class IWorkloadEntityStorage
{
public:
    virtual ~IWorkloadEntityStorage() = default;

    virtual std::string_view getName() const = 0;

    /// Whether this storage can replicate entities to another node.
    virtual bool isReplicated() const { return false; }
    virtual String getReplicationID() const { return ""; }

    /// Loads all entities. Can be called once - if entities are already loaded the function does nothing.
    virtual void loadEntities(const Poco::Util::AbstractConfiguration & config) = 0;

    /// Replaces the implicit entities. Does nothing for storages without an implicit layer.
    virtual void updateImplicitEntities(const ImplicitWorkloadEntitiesConfig & /*config*/) {}

    /// Get visible entity by name. If no entity stored with entity_name throws exception.
    virtual ASTPtr get(const String & entity_name) const = 0;

    /// Get visible entity by name. If no entity stored with entity_name return nullptr.
    virtual ASTPtr tryGet(const String & entity_name) const = 0;

    /// Check if visible entity with entity_name exists.
    virtual bool has(const String & entity_name) const = 0;

    /// Get all visible entities.
    virtual std::vector<std::pair<String, ASTPtr>> getAllEntities() const = 0;

    /// Returns the name of the storage layer owning the visible entity, or an empty string if there is no such entity.
    virtual String getEntitySource(const String & entity_name) const = 0;

    /// Check whether any entity is visible.
    virtual bool empty() const = 0;

    /// Stops watching.
    virtual void stopWatching() {}

    /// Stores an entity.
    virtual bool storeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    /// Removes an entity.
    virtual bool removeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) = 0;

    struct Event
    {
        WorkloadEntityType type;
        String name;
        ASTPtr entity; /// new or changed entity, null if removed
    };
    using OnChangedHandler = std::function<void(const std::vector<Event> &)>;

    /// Gets all visible entities, passes them through `handler` and subscribes for all later transactions.
    /// Destroying the returned guard stops further calls and, if `handler` is already running, waits for it to return.
    /// Destroy it before any state `handler` reads.
    virtual scope_guard getAllEntitiesAndSubscribe(const OnChangedHandler & handler) = 0;

    /// Returns the name of resource used for CPU scheduling of the master query threads
    virtual String getMasterThreadResourceName() = 0;

    /// Returns the name of resource used for CPU scheduling of the additional query threads
    virtual String getWorkerThreadResourceName() = 0;

    /// Returns the name of resource used for query slot scheduling
    virtual String getQueryResourceName() = 0;

    /// Returns the name of resource used for memory reservation
    virtual String getMemoryReservationResourceName() = 0;

    /// Makes backup entries to back up all the workload entities of the specified type.
    virtual void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, WorkloadEntityType entity_type) const = 0;

    /// Restores workload entities of the specified type from a backup.
    virtual void restore(RestorerFromBackup & restorer, const String & data_path_in_backup, WorkloadEntityType entity_type) = 0;
};

}
