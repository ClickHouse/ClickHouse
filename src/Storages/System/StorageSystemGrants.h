#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemGrants final : public IStorageSystemOneBlock<StorageSystemGrants>
{
public:
    std::string getName() const override { return "SystemGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
