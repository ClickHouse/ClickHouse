#include <Common/Scheduler/Workload/createWorkloadEntityStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityDiskStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityKeeperStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityConfigStorage.h>
#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

class MultipleWorkloadEntityStorage : public WorkloadEntityStorageBase
{
public:
    MultipleWorkloadEntityStorage(ContextPtr global_context_)
        : WorkloadEntityStorageBase(global_context_)
    {}

    void addStorage(std::unique_ptr<IWorkloadEntityStorage> storage)
    {
        auto lock = getLock();
        size_t index = storages.size();
        states.emplace_back(); // Add empty state for the new storage
        storages.push_back(std::move(storage));
        subscriptions.push_back(storages.back()->getAllEntitiesAndSubscribe(
            [this, index] (const std::vector<IWorkloadEntityStorage::Event> & events)
            {
                auto lock2 = getLock();
                processEvents(index, events);
            }));
    }

    bool loadEntities(const Poco::Util::AbstractConfiguration & config) override
    {
        auto lock = getLock();
        bool changed = false;
        for (auto & storage : storages)
            changed |= storage->loadEntities(config);
        return changed;
    }

private:
    void processEvents(size_t index, const std::vector<IWorkloadEntityStorage::Event> & events)
    {
        // Update state of the specific storage
        auto & this_state = states[index];
        for (const auto & [entity_type, entity_name, entity] : events)
        {
            if (entity)
                this_state[entity_name] = entity;
            else
                this_state.erase(entity_name);

            // Merge entities from all storages and update/remove it (if necessary)
            ASTPtr merged_entity;
            for (const auto & state : states)
            {
                if (auto entity_it = state.find(entity_name); entity_it != state.end())
                {
                    merged_entity = entity_it->second;
                    break;
                }
            }

            // Update the merged state in memory and notify subscribers
            setOneEntity(entity_name, merged_entity);
        }
    }

    WorkloadEntityStorageBase::OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override
    {
        if (getWritableStorage()->storeEntity(
            current_context,
            entity_type,
            entity_name,
            create_entity_query,
            throw_if_exists,
            replace_if_exists,
            settings))
        {
            return OperationResult::Delegated;
        }
        else
        {
            return OperationResult::Failed;
        }
    }

    WorkloadEntityStorageBase::OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override
    {
        if (getWritableStorage()->removeEntity(
            current_context,
            entity_type,
            entity_name,
            throw_if_not_exists))
        {
            // NOTE: It is important to avoid returning Ok here because DROP may result in CREATE OR REPLACE if entity exists in config storage
            return OperationResult::Delegated;
        }
        else
        {
            return OperationResult::Failed;
        }
    }

    IWorkloadEntityStorage * getWritableStorage()
    {
        chassert(!storages.empty());
        return storages.back().get();
    }

    /// List of storages. The first one is writeable, others are read-only.
    /// The order of storages defines the priority when reading entities.
    std::vector<std::unique_ptr<IWorkloadEntityStorage>> storages;

    /// Complete state of all storages for merging on refresh
    /// Order of maps corresponds to order of storages
    std::vector<std::unordered_map<String, ASTPtr>> states;

    /// Subscriptions to changes in all storages
    std::vector<scope_guard> subscriptions;
};

std::unique_ptr<IWorkloadEntityStorage> createWorkloadEntityStorage(const ContextMutablePtr & global_context)
{
    auto storage = std::make_unique<MultipleWorkloadEntityStorage>(global_context);
    storage->addStorage(std::make_unique<WorkloadEntityConfigStorage>(global_context));

    const String zookeeper_path_key = "workload_zookeeper_path";
    const String disk_path_key = "workload_path";

    const auto & config = global_context->getConfigRef();
    if (config.has(zookeeper_path_key))
    {
        if (config.has(disk_path_key))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'{}' and '{}' must not be both specified in the config",
                zookeeper_path_key,
                disk_path_key);
        }
        storage->addStorage(std::make_unique<WorkloadEntityKeeperStorage>(global_context, config.getString(zookeeper_path_key)));
    }
    else
    {
        String default_path = std::filesystem::path{global_context->getPath()} / "workload" / "";
        String path = config.getString(disk_path_key, default_path);
        storage->addStorage(std::make_unique<WorkloadEntityDiskStorage>(global_context, path));
    }

    return storage;
}

}
