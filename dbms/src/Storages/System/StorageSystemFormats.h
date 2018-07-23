#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageSystemFormats : public ext::shared_ptr_helper<StorageSystemFormats>, public IStorageSystemOneBlock<StorageSystemFormats>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override
    {
        return "SystemFormats";
    }

    static NamesAndTypesList getNamesAndTypes()
    {
        return {
            {"name", std::make_shared<DataTypeString>()},
            {"is_input", std::make_shared<DataTypeUInt8>()},
            {"is_output", std::make_shared<DataTypeUInt8>()},
        };
    }
};
}
