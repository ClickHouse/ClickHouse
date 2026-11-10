#pragma once

#include <list>
#include <mutex>
#include <unordered_map>

#include <base/UUID.h>

#include <Common/Logger_fwd.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// A storage layer (see the terminology in IWorkloadEntityStorage.h): keeps own entities, inherits the visible
/// entities of `next_storage` and exposes the merged visible entities with shadowing by claim keys.
class WorkloadEntityStorageBase : public IWorkloadEntityStorage
{
public:
    explicit WorkloadEntityStorageBase(ContextPtr global_context_, std::unique_ptr<IWorkloadEntityStorage> next_storage_ = {});
    ~WorkloadEntityStorageBase() override;

    ASTPtr get(WorkloadEntityType entity_type, const String & entity_name) const override;

    ASTPtr tryGet(WorkloadEntityType entity_type, const String & entity_name) const override;

    bool has(WorkloadEntityType entity_type, const String & entity_name) const override;

    WorkloadEntities getAllEntities() const override;

    String getEntitySource(WorkloadEntityType entity_type, const String & entity_name) const override;

    bool empty() const override;


    void loadEntities(const Poco::Util::AbstractConfiguration & config) override;

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
    String getMemoryReservationResourceName() override;

    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, WorkloadEntityType entity_type) const override;
    void restore(RestorerFromBackup & restorer, const String & data_path_in_backup, WorkloadEntityType entity_type) override;

protected:
    enum class OperationResult
    {
        Ok,
        Failed,
        Retry,
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

    /// Replaces own entities with loaded `queries` (configuration, Keeper or disk) and notifies subscribers with one
    /// transaction. Throws on invalid entities, conflicts and broken references, keeping the previous entities.
    void setOwnEntities(const ASTs & queries);

    /// Replaces own entities with `new_own_entities` without validation, and notifies subscribers with one
    /// transaction. The caller holds `lock` (see `getLock`) and guarantees that the entities do not conflict.
    /// Unlocks `lock`.
    void replaceOwnEntities(std::unique_lock<std::recursive_mutex> & lock, WorkloadEntities new_own_entities);

    /// Inner layer of the chain, if any.
    IWorkloadEntityStorage * getNextStorage() const { return next_storage.get(); }

    /// Serialize own entities stored in memory plus one optional `change` into multiline string
    String serializeOwnEntities(std::optional<Event> change);

    /// Parses CREATE WORKLOAD and CREATE RESOURCE queries separated by `;`
    static ASTs parseEntitiesFromString(const String & data, LoggerPtr log);

private:
    /// Returns the visible entities for `own` and the current inherited entities
    WorkloadEntities computeVisibleEntities(const WorkloadEntities & own) const;

    /// Recomputes visible entities from own and inherited entities and notifies subscribers about the
    /// difference with one transaction. Unlocks `lock`.
    void updateVisibleEntities(std::unique_lock<std::recursive_mutex> & lock);

    /// Sets the resource names of access-mode roles from visible entities
    void updateRoleResources();

    /// Returns own or inherited entity, inherited one first (it is visible if both exist)
    ASTPtr findKnownEntity(const WorkloadEntityKey & key, const WorkloadEntities & own) const;

    /// Throws if `entity` claims a role also claimed by another own or inherited resource
    void checkClaimConflicts(const WorkloadEntityKey & key, const ASTPtr & entity) const;

    /// Throws if references of loaded `new_own_entities` are broken (see "References" in IWorkloadEntityStorage.h)
    void checkLoadedReferences(const WorkloadEntities & new_own_entities) const;

    /// Notify subscribers about changes described by vector of events `tx`. Takes `notify_mutex` before unlocking
    /// `lock`, so transactions are delivered in the order they were computed.
    void unlockAndNotify(std::unique_lock<std::recursive_mutex> & lock, const std::vector<Event> & tx);

    /// Return true iff own and inherited entities have a reference path from `source` to `target`
    bool isIndirectlyReferenced(const WorkloadEntityKey & target, const WorkloadEntityKey & source) const;

    /// Returns an ordered vector of `entities` with an optional `change` applied
    static std::vector<Event> orderEntities(const WorkloadEntities & entities, const std::optional<Event> & change = {});

    /// Creates all workload entities accumulated from a backup (see restore()) in a proper order, in a single data restore task.
    /// throw_if_exists / replace_if_exists are derived from the create_workloads_and_resources restore setting.
    void restoreEntitiesAccumulatedFromBackup(const ContextMutablePtr & context, const UUID & restore_id, bool throw_if_exists, bool replace_if_exists);

    /// Held by shared_ptr so a subscription outlives both its list node and the storage.
    /// `exec_mutex` is held for the whole invocation, so acquiring it waits for an in-flight call.
    /// `unsubscribed` stops an entry a notifier already copied out of `list` from being invoked.
    struct HandlerEntry
    {
        explicit HandlerEntry(OnChangedHandler handler_) : handler(std::move(handler_)) {}

        OnChangedHandler handler;
        std::mutex exec_mutex;
        bool unsubscribed = false; /// guarded by exec_mutex
    };
    using HandlerEntryPtr = std::shared_ptr<HandlerEntry>;

    struct Handlers
    {
        std::mutex mutex;
        std::list<HandlerEntryPtr> list;
    };
    /// shared_ptr is here for safety because WorkloadEntityStorageBase can be destroyed before all subscriptions are removed.
    std::shared_ptr<Handlers> handlers;

    mutable std::recursive_mutex mutex;
    WorkloadEntities visible_entities; /// The merged view of this layer
    WorkloadEntities own_entities; /// Entities that are stored in this layer, including shadowed ones
    WorkloadEntities inherited_entities; /// Visible entities of the next storage (a copy to be accessed under own mutex)

    /// Serializes deliveries of transactions to subscribers. Taken while `mutex` is held, then held without
    /// `mutex` while handlers run.
    std::mutex notify_mutex;

    // Workload entities collected from a backup before being restored together (see restore()).
    // Keyed by the restore operation's UUID so concurrent restores do not share or overwrite each other's accumulated entities.
    std::unordered_map<UUID, WorkloadEntities> entities_to_restore;

    // A single consistent snapshot of own_entities taken once per backup operation (keyed by the backup UUID),
    // so that system.workloads and system.resources -- backed up via two separate backup() calls on this shared
    // storage -- are captured from the same view (see backup()).
    mutable std::unordered_map<UUID, WorkloadEntities> entities_to_backup;

    String master_thread_resource; /// visible resource for master threads
    String worker_thread_resource; /// visible resource for worker threads
    String query_resource; /// visible resource for queries
    String memory_reservation_resource; /// visible resource for memory reservations

    // Chain of storages
    std::unique_ptr<IWorkloadEntityStorage> next_storage; /// Next storage in the chain (e.g. `disk -> config` or `keeper -> config`)
    scope_guard subscription; /// Subscription to changes in the next storage in the chain, if any

protected:
    ContextPtr global_context;
    LoggerPtr log;
};

}
