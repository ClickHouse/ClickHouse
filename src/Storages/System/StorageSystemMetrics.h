#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `metrics` system table, which provides information about the operation of the server.
  */
class StorageSystemMetrics final : public shared_ptr_helper<StorageSystemMetrics>, public IStorageSystemOneBlock<StorageSystemMetrics>
{
    friend struct shared_ptr_helper<StorageSystemMetrics>;
public:
    std::string getName() const override { return "SystemMetrics"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
