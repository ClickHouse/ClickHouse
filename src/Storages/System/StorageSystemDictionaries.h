#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDictionaries final : public IStorageSystemOneBlock
{
public:
    StorageSystemDictionaries(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemDictionaries"; }

    static ColumnsDescription getColumnsDescription();
    static VirtualColumnsDescription createVirtuals();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
