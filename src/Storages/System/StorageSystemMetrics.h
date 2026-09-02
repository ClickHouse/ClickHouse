#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `metrics` system table, which provides information about the operation of the server.
  */
class StorageSystemMetrics final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemMetrics"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
