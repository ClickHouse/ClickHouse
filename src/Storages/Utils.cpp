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
        /// NOLINT(storage-cast): runs under the database lock, and attach and detach must count a proxy alike.
        if (typeid_cast<StorageReplicatedMergeTree *>(storage.get()) != nullptr)
        {
            return {CurrentMetrics::AttachedTable, CurrentMetrics::AttachedReplicatedTable};
        }
        return {CurrentMetrics::AttachedTable};
    }
}
