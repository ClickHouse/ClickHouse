#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements storage for the system table One.
  * The table contains a single column of dummy UInt8 and a single row with a value of 0.
  * Used when the table is not specified in the query.
  * Analog of the DUAL table in Oracle and MySQL.
  */
class StorageSystemOne final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemOne> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemOne>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemOne(CreatePasskey, TArgs &&... args) : StorageSystemOne{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemOne"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }

protected:
    explicit StorageSystemOne(const StorageID & table_id_);
};

}
