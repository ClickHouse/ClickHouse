#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/// Implements `merge_tree_metadata_cache` system table, which allows you to view the metadata cache data in rocksdb for testing purposes.
class StorageSystemMergeTreeMetadataCache : public IStorageSystemOneBlock<StorageSystemMergeTreeMetadataCache>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemMergeTreeMetadataCache> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemMergeTreeMetadataCache>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemMergeTreeMetadataCache(CreatePasskey, TArgs &&... args) : StorageSystemMergeTreeMetadataCache{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemMergeTreeMetadataCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
#endif
