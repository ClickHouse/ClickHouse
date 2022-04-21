#pragma once

#include <Storages/IStorage.h>


namespace DB
{

/// For system.data_skipping_indices table - describes the data skipping indices in tables, similar to system.columns.
class StorageSystemDataSkippingIndices : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDataSkippingIndices> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDataSkippingIndices>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDataSkippingIndices(CreatePasskey, TArgs &&... args) : StorageSystemDataSkippingIndices{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemDataSkippingIndices"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

protected:
    explicit StorageSystemDataSkippingIndices(const StorageID & table_id_);
};

}
