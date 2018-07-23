#pragma once

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemDataTypeFamilies : public ext::shared_ptr_helper<StorageSystemDataTypeFamilies>,
                                      public IStorageSystemOneBlock<StorageSystemDataTypeFamilies>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override
    {
        return "SystemTableDataTypeFamilies";
    }

    static NamesAndTypesList getNamesAndTypes()
    {
        return {
            {"name", std::make_shared<DataTypeString>()},
            {"case_insensivie", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"alias_to", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"parametric", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"have_subtypes", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"cannot_be_stored_in_tables", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"comparable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"can_be_compared_with_collation", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"can_be_used_as_version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"is_summable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"can_be_used_in_bit_operations", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"can_be_used_in_boolean_context", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"categorial", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"nullable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"only_null", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
            {"can_be_inside_nullable", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
        };
    }
};
}
