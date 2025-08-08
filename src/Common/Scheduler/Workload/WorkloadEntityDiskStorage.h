#pragma once

#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Loads workload entities from a specified folder.
class WorkloadEntityDiskStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityDiskStorage(const ContextPtr & global_context_, const String & dir_path_);
    void loadEntities() override;

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

    void createDirectory();
    void loadEntitiesImpl();
    ASTPtr tryLoadEntity(WorkloadEntityType entity_type, const String & entity_name);
    ASTPtr tryLoadEntity(WorkloadEntityType entity_type, const String & entity_name, const String & file_path, bool check_file_exists);
    String getFilePath(WorkloadEntityType entity_type, const String & entity_name) const;

    String dir_path;
    std::atomic<bool> entities_loaded = false;
};

}
