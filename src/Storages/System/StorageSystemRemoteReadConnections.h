#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.remote_read_connections — shows active remote read connections held by ReaderExecutor.
class StorageSystemRemoteReadConnections final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemRemoteReadConnections"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
