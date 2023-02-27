#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDictionaries final : public shared_ptr_helper<StorageSystemDictionaries>, public IStorageSystemOneBlock<StorageSystemDictionaries>
{
    friend struct shared_ptr_helper<StorageSystemDictionaries>;
public:
    std::string getName() const override { return "SystemDictionaries"; }

    static NamesAndTypesList getNamesAndTypes();

    NamesAndTypesList getVirtuals() const override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
