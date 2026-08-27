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
        /// Asked while attaching, so this must not load a lazy table. `getName` answers from the
        /// `CREATE` query in that case, whereas a cast would see only the proxy.
        const auto engine = storage->getName();
        if (engine.starts_with("Replicated") && engine.ends_with("MergeTree"))
        {
            return {CurrentMetrics::AttachedTable, CurrentMetrics::AttachedReplicatedTable};
        }
        return {CurrentMetrics::AttachedTable};
    }
}
