#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `metrics` system table, which provides information about the operation of the server.
  */
class StorageSystemMetrics : public ext::shared_ptr_helper<StorageSystemMetrics>, public IStorageSystemOneBlock<StorageSystemMetrics>
{
public:
    std::string getName() const override { return "SystemMetrics"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
