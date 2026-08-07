#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `processes` system table, which allows you to get information about the queries that are currently executing.
  */
class StorageSystemProcesses final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemProcesses"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
