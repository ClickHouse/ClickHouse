#pragma once


#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.dns_cache table.
class StorageSystemDNSCache final : public IStorageSystemOneBlock<StorageSystemDNSCache>
{
public:
    std::string getName() const override { return "SystemDNSCache"; }
    ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
