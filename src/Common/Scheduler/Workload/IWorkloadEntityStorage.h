#pragma once

#include <base/types.h>
#include <base/scope_guard.h>

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

enum class WorkloadEntityType : uint8_t
{
    Workload,
    Resource,

    MAX
};

/// Names of the server-synthesized (implicit) resources created by `WorkloadResourceManager` when
/// the corresponding `workloads_respect_server_*_limit` setting is enabled and the operator declared
/// no matching resource. They are internal: never persisted, never exposed as user entities, and
/// chosen with reserved-style delimiters so an ordinary `CREATE RESOURCE` name is unlikely to collide.
/// The manager (which creates the resource) and the storage (which resolves the resource name for the
/// execution paths) share these constants so both refer to the same resource.
inline constexpr std::string_view IMPLICIT_CPU_RESOURCE_NAME = "__server_cpu__";
inline constexpr std::string_view IMPLICIT_MEMORY_RESOURCE_NAME = "__server_memory__";

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

    /// Get entity by name. If no entity stored with entity_name throws exception.
    virtual ASTPtr get(const String & entity_name) const = 0;

    /// Get entity by name. If no entity stored with entity_name return nullptr.
    virtual ASTPtr tryGet(const String & entity_name) const = 0;

    /// Check if entity with entity_name is stored.
    virtual bool has(const String & entity_name) const = 0;

    /// Get all entities.
    virtual std::vector<std::pair<String, ASTPtr>> getAllEntities() const = 0;

    /// Check whether any entity have been stored.
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

    /// Gets all current entries, pass them through `handler` and subscribes for all later changes.
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

    /// Records whether the server-limit workload features are enabled. When enabled and the operator
    /// declared no matching resource, the resource-name getters above resolve to the implicit
    /// server-synthesized resource (`IMPLICIT_CPU_RESOURCE_NAME` / `IMPLICIT_MEMORY_RESOURCE_NAME`)
    /// that `WorkloadResourceManager` creates, so the execution paths route through it.
    virtual void setServerLimitsEnabled(bool /*respect_cpu_limit*/, bool /*respect_memory_limit*/) {}

    /// Makes backup entries to back up all the workload entities of the specified type.
    virtual void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, WorkloadEntityType entity_type) const = 0;

    /// Restores workload entities of the specified type from a backup.
    virtual void restore(RestorerFromBackup & restorer, const String & data_path_in_backup, WorkloadEntityType entity_type) = 0;
};

}
