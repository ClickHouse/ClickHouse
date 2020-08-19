#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Implements `row_policies` system table, which allows you to get information about row policies.
class StorageSystemRowPolicies final : public ext::shared_ptr_helper<StorageSystemRowPolicies>, public IStorageSystemOneBlock<StorageSystemRowPolicies>
{
public:
    std::string getName() const override { return "SystemRowPolicies"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemRowPolicies>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
