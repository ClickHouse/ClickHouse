#pragma once

#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// The outermost layer of the server chain (see IWorkloadEntityStorage.h). Its own entities are the implicit
/// entities synthesized from `ImplicitWorkloadEntitiesConfig`, including the root workload. Writes, backup and
/// restore are forwarded to the next (SQL) storage.
class WorkloadEntityImplicitStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityImplicitStorage(const ContextPtr & global_context_, std::unique_ptr<IWorkloadEntityStorage> next_storage_);

    std::string_view getName() const override { return "implicit"; }

    bool isReplicated() const override;
    String getReplicationID() const override;
    void stopWatching() override;

    void updateImplicitEntities(const ImplicitWorkloadEntitiesConfig & new_config) override;

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

    void backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, WorkloadEntityType entity_type) const override;
    void restore(RestorerFromBackup & restorer, const String & data_path_in_backup, WorkloadEntityType entity_type) override;

private:
    OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override;

    /// Whether the server settings define an implicit entity of this kind and name, visible or shadowed
    bool definesEntity(const String & entity_name, WorkloadEntityType entity_type) const;

    ImplicitWorkloadEntitiesConfig config; /// Guarded by the storage lock
};

}
