#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemFormats final : public IStorageSystemOneBlock<StorageSystemFormats>
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemFormats";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
