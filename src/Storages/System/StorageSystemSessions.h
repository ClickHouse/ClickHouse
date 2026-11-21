#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `sessions` system table, which allows you to get information about currently logged-in sessions.
  */
class StorageSystemSessions final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemSessions"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
