#pragma once

#include <DataTypes/DataTypeString.h>
#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Information about macros for introspection.
  */
class StorageSystemMacros final : public shared_ptr_helper<StorageSystemMacros>, public IStorageSystemOneBlock<StorageSystemMacros>
{
    friend struct shared_ptr_helper<StorageSystemMacros>;
public:
    std::string getName() const override { return "SystemMacros"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
