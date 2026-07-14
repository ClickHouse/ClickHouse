#pragma once

#include <unordered_map>
#include <list>
#include <mutex>
#include <unordered_set>

#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

class WorkloadEntityStorageBase : public IWorkloadEntityStorage
{
public:
    explicit WorkloadEntityStorageBase(ContextPtr global_context_);
    ASTPtr get(const String & entity_name) const override;

    ASTPtr tryGet(const String & entity_name) const override;

    bool has(const String & entity_name) const override;

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

    scope_guard getAllEntitiesAndSubscribe(
        const OnChangedHandler & handler) override;

    String getMasterThreadResourceName() override;
    String getWorkerThreadResourceName() override;
    String getQueryResourceName() override;

protected:
    enum class OperationResult
    {
        Ok,
        Failed,
        Retry
    };

    virtual OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) = 0;

    virtual OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) = 0;

    std::unique_lock<std::recursive_mutex> getLock() const;

    /// Replace current `entities` with `new_entities` and notifies subscribers.
    /// Note that subscribers will be notified with a sequence of events.
    /// It is guaranteed that all itermediate states (between every pair of consecutive events)
    /// will be consistent (all references between entities will be valid)
    void setAllEntities(const std::vector<std::pair<String, ASTPtr>> & new_entities);

    /// Serialize `entities` stored in memory plus one optional `change` into multiline string
    String serializeAllEntities(std::optional<Event> change = {});

private:
    /// Change state in memory
    void applyEvent(std::unique_lock<std::recursive_mutex> & lock, const Event & event);

    /// Notify subscribers about changes describe by vector of events `tx`
    void unlockAndNotify(std::unique_lock<std::recursive_mutex> & lock, std::vector<Event> tx);

    /// Return true iff `references` has a path from `source` to `target`
    bool isIndirectlyReferenced(const String & target, const String & source);

    /// Adds references that are described by `entity` to `references`
    void insertReferences(const ASTPtr & entity);

    /// Removes references that are described by `entity` from `references`
    void removeReferences(const ASTPtr & entity);

    /// Returns an ordered vector of `entities`
    std::vector<Event> orderEntities(
        const std::unordered_map<String, ASTPtr> & all_entities,
        std::optional<Event> change = {});

    struct Handlers
    {
        std::mutex mutex;
        std::list<OnChangedHandler> list;
    };
    /// shared_ptr is here for safety because WorkloadEntityStorageBase can be destroyed before all subscriptions are removed.
    std::shared_ptr<Handlers> handlers;

    mutable std::recursive_mutex mutex;
    std::unordered_map<String, ASTPtr> entities; /// Maps entity name into CREATE entity query

    // Validation
    std::unordered_map<String, std::unordered_set<String>> references; /// Keep track of references between entities. Key is target. Value is set of sources
    String root_name; /// current root workload name
    String master_thread_resource; /// current resource name for worker threads
    String worker_thread_resource; /// current resource name for master threads
    String query_resource; /// current resource name for queries

protected:
    ContextPtr global_context;
    LoggerPtr log;
};

}
