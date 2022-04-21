#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the system table `tables`, which allows you to get information about all tables.
  */
class StorageSystemTables final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemTables> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemTables>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemTables(CreatePasskey, TArgs &&... args) : StorageSystemTables{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemTables"; }

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
    explicit StorageSystemTables(const StorageID & table_id_);
};

}
