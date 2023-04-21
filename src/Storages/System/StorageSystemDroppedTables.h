#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class StorageSystemDroppedTables final : public IStorageSystemOneBlock<StorageSystemDroppedTables>
{
public:
    std::string getName() const override { return "SystemMarkedDroppedTables"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
