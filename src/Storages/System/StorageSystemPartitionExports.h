#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/// system.partition_exports: progress of EXPORT PARTITION tasks of every MergeTree-family table,
/// both plain `MergeTree` (backed by on-disk task descriptors) and `Replicated*MergeTree` (backed
/// by the ZooKeeper manifest mirror). Both are read from memory, so querying it touches neither
/// disk nor ZooKeeper. Each export task is represented by a single row.
///
/// Also attached as `system.replicated_partition_exports`, a backwards-compatible alias from when
/// the two engines had separate tables.
class StorageSystemPartitionExports final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemPartitionExports"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
