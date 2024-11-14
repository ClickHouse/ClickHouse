#include <vector>
#include <Storages/Utils.h>
#include <Storages/IStorage.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace CurrentMetrics
{
    extern const Metric AttachedTable;
    extern const Metric AttachedReplicatedTable;
    extern const Metric AttachedView;
    extern const Metric AttachedDictionary;
}


namespace DB
{
    std::vector<CurrentMetrics::Metric> getAttachedCountersForStorage(const StoragePtr & storage)
    {
        if (storage->isView())
        {
            return {CurrentMetrics::AttachedView};
        }
        if (storage->isDictionary())
        {
            return {CurrentMetrics::AttachedDictionary};
        }
        if (auto * replicated_storage = typeid_cast<StorageReplicatedMergeTree *>(storage.get()))
        {
            return {CurrentMetrics::AttachedTable, CurrentMetrics::AttachedReplicatedTable};
        }
        return {CurrentMetrics::AttachedTable};
    }
}
