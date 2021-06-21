#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageSystemFormats final : public ext::shared_ptr_helper<StorageSystemFormats>, public IStorageSystemOneBlock<StorageSystemFormats>
{
    friend struct ext::shared_ptr_helper<StorageSystemFormats>;
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override
    {
        return "SystemFormats";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
