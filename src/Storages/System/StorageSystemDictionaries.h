#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDictionaries final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDictionaries"; }

    static ColumnsDescription getColumnsDescription();

    NamesAndTypesList getVirtuals() const override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
