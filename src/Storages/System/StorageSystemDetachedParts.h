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
class StorageSystemDetachedParts final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDetachedParts> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDetachedParts>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDetachedParts(CreatePasskey, TArgs &&... args) : StorageSystemDetachedParts{std::forward<TArgs>(args)...}
    {
    }

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
