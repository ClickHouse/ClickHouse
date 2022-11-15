#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `rocksdb` system table, which expose various rocksdb metrics.
  */
class StorageSystemRocksDB final : public IStorageSystemOneBlock<StorageSystemRocksDB>
{
public:
    std::string getName() const override { return "SystemRocksDB"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
