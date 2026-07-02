#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/PartitionTxnController.h>

#include <base/types.h>

#include <memory>

namespace DB
{
    class MergeTreeData;
}

namespace DB::UniqueKeyTxn
{

/// Build a `PartitionTxnController` wired with the `LocalXxx` strategies (the
/// bitmap store is a `MergeTreeBitmapStore` in resolution-context mode). The
/// controller reads the partition's live state from `data` + `partition_id`
/// directly. Caller owns the returned controller and ties it to the partition's
/// lifetime (storage map `unique_key_txn_controllers`); `data` must outlive
/// the controller.
std::unique_ptr<PartitionTxnController> MakeLocalStrategies(
    const MergeTreeData & data,
    const String &  partition_id);

}
