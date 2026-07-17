#pragma once

#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{

class DataTypeDate final : public DataTypeNumberBase<UInt16>
{
public:
    static constexpr auto family_name = "Date";

    TypeIndex getTypeId() const override { return TypeIndex::Date; }
    TypeIndex getColumnType() const override { return TypeIndex::UInt16; }
    const char * getFamilyName() const override { return family_name; }

    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;

protected:
    SerializationPtr doGetDefaultSerialization() const override;
};

}
