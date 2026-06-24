#include <Storages/MergeTree/UniqueKey/Txn/MakeLocalStrategies.h>

#include <Storages/MergeTree/UniqueKey/Txn/LocalStrategies.h>
#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>

#include <Storages/MergeTree/UniqueKey/DeleteBitmapFileOps.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Interpreters/Context.h>

#include <algorithm>
#include <memory>

namespace DB::UniqueKeyTxn
{

namespace
{
    /// Partition.csn floor from per-part state: max over active parts of
    /// `max(creation_csn, highest on-disk bitmap version)`. `INVALID_CSN` (0)
    /// for an empty partition. Both terms are CSN-based, so the first commit's
    /// csn exceeds every prior on-disk bitmap version (csn-named sidecars) and
    /// every active part's recorded `creation_csn`.
    CSN computeCsnSeed(const MergeTreeData & data, const String & partition_id)
    {
        auto shared = data.getActivePartsInPartitionShared(partition_id);
        if (!shared)
            return INVALID_CSN;

        CSN floor = INVALID_CSN;
        for (const auto & p : *shared)
        {
            if (!p)
                continue;
            if (auto meta = p->getUniqueKeyMeta())
                floor = std::max(floor, meta->creation_csn);
            floor = std::max(floor,
                static_cast<CSN>(DeleteBitmapFileOps::getLastVersionFromStorage(p->getDataPartStorage())));
        }
        return floor;
    }
}

std::unique_ptr<PartitionTxnController> MakeLocalStrategies(
    const MergeTreeData & data,
    const String &  partition_id)
{
    /// Coordinator — owns the publish lock + in-memory csn.
    auto coordinator = std::make_unique<LocalCommitCoordinator>();

    /// Seed `partition.csn` from existing partition state so the first commit's
    /// csn exceeds every prior on-disk version.
    coordinator->seedCsn(computeCsnSeed(data, partition_id));

    /// Shared handle to the (process-wide) Context cache; null when the
    /// delete-bitmap cache is unregistered, which the store tolerates
    /// (read-through). Co-owned with the Context, which outlives the table.
    DeleteBitmapCachePtr bitmap_cache;
    if (auto context = data.getContext())
        bitmap_cache = context->getDeleteBitmapCache();

    /// `MergeTreeBitmapStore` doubles as the Local `IBitmapStore`: the
    /// resolution-context ctor lets its `PartName`-keyed overrides resolve
    /// over `data` + `partition_id`.
    auto bitmap_store = std::make_unique<MergeTreeBitmapStore>(data, partition_id, bitmap_cache);
    auto pin_registry = std::make_unique<LocalPinRegistry>();

    return std::make_unique<PartitionTxnController>(
        std::move(coordinator),
        std::move(bitmap_store),
        std::move(pin_registry));
}

}
