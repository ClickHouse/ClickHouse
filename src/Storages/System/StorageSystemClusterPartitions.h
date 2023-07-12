#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// system.cluster_partitions
class StorageSystemClusterPartitions final : public IStorageSystemOneBlock<StorageSystemClusterPartitions>
{
public:
    std::string getName() const override { return "StorageSystemClusterPartitions"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
