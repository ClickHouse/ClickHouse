#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `connections` system table.
  * This table shows the open client connections of the native TCP and HTTP query interfaces.
  */
class StorageSystemConnections final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemConnections"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
