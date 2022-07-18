#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/// Implements `merge_tree_metadata_cache` system table, which allows you to view the metadata cache data in rocksdb for testing purposes.
class StorageSystemMergeTreeMetadataCache : public shared_ptr_helper<StorageSystemMergeTreeMetadataCache>, public IStorageSystemOneBlock<StorageSystemMergeTreeMetadataCache>
{
    friend struct shared_ptr_helper<StorageSystemMergeTreeMetadataCache>;

public:
    std::string getName() const override { return "SystemMergeTreeMetadataCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
#endif
