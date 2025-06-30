#pragma once

#include <DataTypes/IDataType.h>
#include <Common/SipHash.h>


namespace DB
{

class ColumnString;

class DataTypeString final : public IDataType
{
public:
    using FieldType = String;
    using ColumnType = ColumnString;
    static constexpr bool is_parametric = false;
    static constexpr auto type_id = TypeIndex::String;

    const char * getFamilyName() const override
    {
        return "String";
    }

    TypeIndex getTypeId() const override { return type_id; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash &) const override {}

    SerializationPtr doGetDefaultSerialization() const override;
};

}
