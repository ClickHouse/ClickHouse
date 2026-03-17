#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `connections` system table, which allows you to see information about
  * currently active client connections to the server (TCP and HTTP protocols).
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
