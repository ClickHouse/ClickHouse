#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `rocksdb` system table, which expose various rocksdb metrics.
  */
class StorageSystemRocksDB final : public IStorageSystemOneBlock<StorageSystemRocksDB>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemRocksDB> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemRocksDB>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemRocksDB(CreatePasskey, TArgs &&... args) : StorageSystemRocksDB{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemRocksDB"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
