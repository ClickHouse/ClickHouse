#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageSystemCollations : public ext::shared_ptr_helper<StorageSystemCollations>,
                                public IStorageSystemOneBlock<StorageSystemCollations>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override
    {
        return "SystemTableCollations";
    }

    static NamesAndTypesList getNamesAndTypes()
    {
        return {
            {"name", std::make_shared<DataTypeString>()},
        };
    }
};
}
