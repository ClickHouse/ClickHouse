#pragma once

#include <base/types.h>
#include <base/scope_guard.h>

#include <compare>
#include <limits>
#include <map>
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
 * Entity. A WORKLOAD or a RESOURCE, represented by its normalized CREATE query (without `OR REPLACE` / `IF NOT
 * EXISTS`) and identified by its kind and name (`WorkloadEntityKey`). A workload and a resource may have the same
 * name.
 *
 * Chain. Storage layers are linked through `next_storage`: every layer wraps the next (inner) one. The server chain,
 * from the outermost to the innermost layer, is:
 *   implicit (`WorkloadEntityImplicitStorage`)
 *     -> SQL, persisted (`WorkloadEntityKeeperStorage` or `WorkloadEntityDiskStorage`)
 *       -> configuration (`WorkloadEntityConfigStorage`)
 * Readers (the resource manager, disks, system tables, interpreters) work with the outermost layer.
 *
 * Own, inherited and visible entities. The own entities of a layer are stored in it (written by SQL, loaded from the
 * configuration, Keeper or disk, or synthesized by the implicit layer). Its inherited entities are the visible
 * entities of the inner layer. Its visible entities are the inherited entities plus the own entities that are not
 * shadowed. The inner layer has priority: configuration > SQL > implicit.
 *
 * Claim keys. A workload claims its key. A resource claims its key and one key per access-mode role it takes: MASTER
 * THREAD, WORKER THREAD, QUERY and MEMORY RESERVATION are served by at most one resource each (see
 * `getMasterThreadResourceName` and friends). READ DISK and WRITE DISK claim nothing.
 *
 * Shadowing. An own entity is shadowed when an inherited entity claims one of its claim keys. A shadowed entity stays
 * stored but is not visible, as a whole; it is visible again once the inherited entity goes away.
 *
 * Conflicts. Two own entities of one layer that share a claim key: the same kind and name, or two resources for one
 * role. A SQL statement also conflicts with an inherited entity sharing a claim key. Statements and loads reject
 * conflicts with an exception.
 *
 * Implicit entities. The implicit layer synthesizes from the server configuration (see
 * `ImplicitWorkloadEntitiesConfig`) the resources `__server_cpu__` (MASTER THREAD, WORKER THREAD) and
 * `__server_memory__` (MEMORY RESERVATION), the workload `default` and the root workload (see below). User entities
 * shadow them. The names of the implicit resources are reserved: statements and loads reject user entities with these
 * names. Implicit entities cannot be dropped.
 *
 * Root workload. The workload with the empty name. It is the parent of every workload without an explicit parent.
 * The implicit layer always provides it; it has no SQL form. Its settings are the server limits without `FOR` (e.g.
 * `max_concurrent_threads = 8`), so they apply to every resource of the matching unit. The resource manager creates
 * no workload node for it and applies its settings to the root node of every resource.
 *
 * Events and transactions. Subscribers of a layer receive changes of its visible entities only. Every change in any
 * layer is turned by every outer layer into one transaction that turns the old visible entities into the new ones.
 * An unchanged view produces no transaction. Transactions, including the initial snapshot of a new subscriber, are
 * delivered one at a time in the order they were produced. Handlers are called without the layer's lock, except on
 * the Keeper refresh paths, where `WorkloadEntityKeeperStorage` holds its lock. A handler must not call back into the
 * layer that notifies it.
 *
 * References. A workload references its parent workload and the resources named in its `FOR` clauses. A layer
 * resolves references against its own and inherited entities, so the configuration references only its own entities
 * and SQL entities may reference configuration entities, but not implicit ones. Every statement and every load
 * (configuration, Keeper, disk) rejects a missing target and a parent cycle; a configuration reload is then not
 * applied, and a failed Keeper or disk load fails the startup or the refresh. A configuration reload is not checked
 * against the SQL layer: dropping a configuration entity that a SQL entity references breaks the reference, and the
 * next load of the SQL entities fails until it is fixed.
 *
 * Writes, backup and restore. `storeEntity`, `removeEntity`, `backup` and `restore` are served by the SQL layer: the
 * implicit layer forwards them, and the configuration is read-only. A backup contains the own entities of the SQL
 * layer. A restore recreates them in the SQL layer through `storeEntity`.
 */

enum class WorkloadEntityType : uint8_t
{
    Workload,
    Resource,

    MAX
};

/// Identifies a workload entity (see above)
struct WorkloadEntityKey
{
    WorkloadEntityType type;
    String name;

    auto operator<=>(const WorkloadEntityKey &) const = default;
};

/// Maps a workload entity into its CREATE query
using WorkloadEntities = std::map<WorkloadEntityKey, ASTPtr>;

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

    /// `max_concurrent_threads` of the root workload.
    Int64 cpu_slots = unlimited;

    /// `max_memory` of the root workload, in bytes.
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

    /// Get visible entity. Throws if there is no such entity.
    virtual ASTPtr get(WorkloadEntityType entity_type, const String & entity_name) const = 0;

    /// Get visible entity. Returns nullptr if there is no such entity.
    virtual ASTPtr tryGet(WorkloadEntityType entity_type, const String & entity_name) const = 0;

    /// Check if visible entity exists.
    virtual bool has(WorkloadEntityType entity_type, const String & entity_name) const = 0;

    /// Get all visible entities.
    virtual WorkloadEntities getAllEntities() const = 0;

    /// Returns the name of the storage layer owning the visible entity, or an empty string if there is no such entity.
    virtual String getEntitySource(WorkloadEntityType entity_type, const String & entity_name) const = 0;

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
