#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/**
  * Implements system table 'detached_parts' which allows to get information
  * about detached data parts for tables of MergeTree family.
  * We don't use StorageSystemPartsBase, because it introduces virtual _state
  * column and column aliases which we don't need.
  */
class StorageSystemDetachedParts final :
        public shared_ptr_helper<StorageSystemDetachedParts>,
        public IStorage
{
    friend struct shared_ptr_helper<StorageSystemDetachedParts>;
public:
    std::string getName() const override { return "SystemDetachedParts"; }
    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemDetachedParts(const StorageID & table_id_);

    Pipe read(
            const Names & /* column_names */,
            const StorageSnapshotPtr & storage_snapshot,
            SelectQueryInfo & query_info,
            ContextPtr context,
            QueryProcessingStage::Enum /*processed_stage*/,
            size_t /*max_block_size*/,
            unsigned /*num_streams*/) override;
};

}
