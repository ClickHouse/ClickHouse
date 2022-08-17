#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemGrants final : public shared_ptr_helper<StorageSystemGrants>, public IStorageSystemOneBlock<StorageSystemGrants>
{
public:
    std::string getName() const override { return "SystemGrants"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemGrants>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
