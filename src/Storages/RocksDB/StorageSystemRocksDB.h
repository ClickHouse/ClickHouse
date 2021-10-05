#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `rocksdb` system table, which expose various rocksdb metrics.
  */
class StorageSystemRocksDB final : public shared_ptr_helper<StorageSystemRocksDB>, public IStorageSystemOneBlock<StorageSystemRocksDB>
{
    friend struct shared_ptr_helper<StorageSystemRocksDB>;
public:
    std::string getName() const override { return "SystemRocksDB"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
