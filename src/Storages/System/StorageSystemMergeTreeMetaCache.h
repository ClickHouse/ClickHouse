#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** Implements `merge_tree_meta_cache` system table, which allows you to view the metacache data in rocksdb for debugging purposes.
  */
class StorageSystemMergeTreeMetaCache : public shared_ptr_helper<StorageSystemMergeTreeMetaCache>, public IStorageSystemOneBlock<StorageSystemMergeTreeMetaCache>
{
    friend struct shared_ptr_helper<StorageSystemMergeTreeMetaCache>;

public:
    std::string getName() const override { return "SystemMergeTreeMetaCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
#endif
