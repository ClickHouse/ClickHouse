#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `distribution_queue` system table, which allows you to view the INSERT queues for the Distributed tables.
  */
class StorageSystemErrors final : public ext::shared_ptr_helper<StorageSystemErrors>, public IStorageSystemOneBlock<StorageSystemErrors>
{
    friend struct ext::shared_ptr_helper<StorageSystemErrors>;
public:
    std::string getName() const override { return "SystemErrors"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const override;
};

}
