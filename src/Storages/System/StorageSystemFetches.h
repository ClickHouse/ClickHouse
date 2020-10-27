#pragma once


#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.fetches table. Takes data from context.getReplicatedFetchList()
class StorageSystemFetches final : public ext::shared_ptr_helper<StorageSystemFetches>, public IStorageSystemOneBlock<StorageSystemFetches >
{
    friend struct ext::shared_ptr_helper<StorageSystemFetches>;
public:
    std::string getName() const override { return "SystemFetches"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
