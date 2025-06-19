#pragma once

#include <base/types.h>
#include <base/scope_guard.h>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>


namespace DB
{

class IAST;
struct Settings;

enum class WorkloadEntityType : uint8_t
{
    Workload,
    Resource,

    MAX
};

/// Interface for a storage of workload entities (WORKLOAD and RESOURCE).
class IWorkloadEntityStorage
{
public:
    virtual ~IWorkloadEntityStorage() = default;

    /// Whether this storage can replicate entities to another node.
    virtual bool isReplicated() const { return false; }
    virtual String getReplicationID() const { return ""; }

    /// Loads all entities. Can be called once - if entities are already loaded the function does nothing.
    virtual void loadEntities() = 0;

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
    virtual scope_guard getAllEntitiesAndSubscribe(const OnChangedHandler & handler) = 0;

    /// Returns the name of resource used for CPU scheduling of the master query threads
    virtual String getMasterThreadResourceName() = 0;

    /// Returns the name of resource used for CPU scheduling of the additional query threads
    virtual String getWorkerThreadResourceName() = 0;

    /// Returns the name of resource used for query slot scheduling
    virtual String getQueryResourceName() = 0;
};

}
