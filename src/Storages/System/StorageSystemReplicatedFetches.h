#pragma once


#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.replicated_fetches table. Takes data from context.getReplicatedFetchList()
class StorageSystemReplicatedFetches final : public ext::shared_ptr_helper<StorageSystemReplicatedFetches>, public IStorageSystemOneBlock<StorageSystemReplicatedFetches >
{
    friend struct ext::shared_ptr_helper<StorageSystemReplicatedFetches>;
public:
    std::string getName() const override { return "SystemReplicatedFetches"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
