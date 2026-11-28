#pragma once

#include <Core/Field.h>
#include <DataTypes/DataTypeNumberBase.h>

namespace DB
{
class DataTypeDate32 final : public DataTypeNumberBase<Int32>
{
public:
    static constexpr auto family_name = "Date32";

    TypeIndex getTypeId() const override { return TypeIndex::Date32; }
    TypeIndex getColumnType() const override { return TypeIndex::Int32; }
    const char * getFamilyName() const override { return family_name; }

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;
    void insertManyDefaultsInto(IColumn & column, size_t n) const override;
    bool isDefaultInsertTrivial() const override { return false; }

    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;

protected:
    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override;
};
}
