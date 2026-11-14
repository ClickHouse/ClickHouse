#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `distributed_bucket_maps` system table, which shows the bucket maps loaded by the Distributed tables.
  */
class StorageSystemDistributedBucketMaps final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDistributedBucketMaps"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
