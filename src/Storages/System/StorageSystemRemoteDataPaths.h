#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemRemoteDataPaths : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemRemoteDataPaths> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemRemoteDataPaths>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemRemoteDataPaths(CreatePasskey, TArgs &&... args) : StorageSystemRemoteDataPaths{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemRemoteDataPaths"; }

    bool isSystemStorage() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    explicit StorageSystemRemoteDataPaths(const StorageID & table_id_);
};

}
