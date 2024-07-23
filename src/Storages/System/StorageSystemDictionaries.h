#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDictionaries final : public IStorageSystemOneBlock<StorageSystemDictionaries>
{
public:
    std::string getName() const override { return "SystemDictionaries"; }

    static NamesAndTypesList getNamesAndTypes();

    NamesAndTypesList getVirtuals() const override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
