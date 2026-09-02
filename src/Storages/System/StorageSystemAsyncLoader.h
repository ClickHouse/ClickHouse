#pragma once


#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.asynchronous_loader table. Takes data from context.getAsyncLoader()
class StorageSystemAsyncLoader final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemAsyncLoader"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
