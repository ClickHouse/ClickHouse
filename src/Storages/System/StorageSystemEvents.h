#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;


/** Implements `events` system table, which allows you to obtain information for profiling.
  */
class StorageSystemEvents final : public ext::shared_ptr_helper<StorageSystemEvents>, public IStorageSystemOneBlock<StorageSystemEvents>
{
    friend struct ext::shared_ptr_helper<StorageSystemEvents>;
public:
    std::string getName() const override { return "SystemEvents"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
