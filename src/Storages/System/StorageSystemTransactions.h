#pragma once
#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemTransactions final : public shared_ptr_helper<StorageSystemTransactions>, public IStorageSystemOneBlock<StorageSystemTransactions>
{
    friend struct shared_ptr_helper<StorageSystemTransactions>;
public:
    String getName() const override { return "SystemTransactions"; }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases() { return {}; }

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
