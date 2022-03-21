#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemRemoteFilesystemCache final : public shared_ptr_helper<StorageSystemRemoteFilesystemCache>,
    public IStorageSystemOneBlock<StorageSystemRemoteFilesystemCache>
{
    friend struct shared_ptr_helper<StorageSystemRemoteFilesystemCache>;
public:
    std::string getName() const override { return "SystemRemoteFilesystemCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    StorageSystemRemoteFilesystemCache(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
