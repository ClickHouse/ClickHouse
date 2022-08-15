#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `functions`system table, which allows you to get a list
  * all normal and aggregate functions.
  */
class StorageSystemFunctions final : public shared_ptr_helper<StorageSystemFunctions>, public IStorageSystemOneBlock<StorageSystemFunctions>
{
    friend struct shared_ptr_helper<StorageSystemFunctions>;
public:
    std::string getName() const override { return "SystemFunctions"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
