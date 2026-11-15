#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.dns_cache table.
class StorageSystemDNSCache final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDNSCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
