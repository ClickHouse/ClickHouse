#pragma once

#include <unordered_map>
#include <list>
#include <mutex>
#include <queue>

#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST.h>

namespace DB
{

class WorkloadEntityStorageBase : public IWorkloadEntityStorage
{
public:
    explicit WorkloadEntityStorageBase(ContextPtr global_context_);
    ASTPtr get(const String & entity_name) const override;

    ASTPtr tryGet(const String & entity_name) const override;

    bool has(const String & entity_name) const override;

    std::vector<String> getAllEntityNames() const override;

    std::vector<std::pair<String, ASTPtr>> getAllEntities() const override;

    bool empty() const override;

    bool storeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    bool removeEntity(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override;

    virtual scope_guard subscribeForChanges(
        WorkloadEntityType entity_type,
        const OnChangedHandler & handler) override;

protected:
    virtual bool storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    virtual bool removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) = 0;

    std::unique_lock<std::recursive_mutex> getLock() const;
    void setAllEntities(const std::vector<std::pair<String, ASTPtr>> & new_entities);
    void removeAllEntitiesExcept(const Strings & entity_names_to_keep);

    /// Called by derived class after a new workload entity has been added.
    void onEntityAdded(WorkloadEntityType entity_type, const String & entity_name, const ASTPtr & new_entity);

    /// Called by derived class after an workload entity has been changed.
    void onEntityUpdated(WorkloadEntityType entity_type, const String & entity_name, const ASTPtr & changed_entity);

    /// Called by derived class after an workload entity has been removed.
    void onEntityRemoved(WorkloadEntityType entity_type, const String & entity_name);

    /// Sends notifications to subscribers about changes in workload entities
    /// (added with previous calls onEntityAdded(), onEntityUpdated(), onEntityRemoved()).
    void sendNotifications();

    struct Handlers
    {
        std::mutex mutex;
        std::list<OnChangedHandler> by_type[static_cast<size_t>(WorkloadEntityType::MAX)];
    };
    /// shared_ptr is here for safety because WorkloadEntityStorageBase can be destroyed before all subscriptions are removed.
    std::shared_ptr<Handlers> handlers;

    struct Event
    {
        WorkloadEntityType type;
        String name;
        ASTPtr entity;
    };
    std::queue<Event> queue;
    std::mutex queue_mutex;
    std::mutex sending_notifications;

    mutable std::recursive_mutex mutex;
    std::unordered_map<String, ASTPtr> entities; // Maps entity name into CREATE entity query

    ContextPtr global_context;
};

}
