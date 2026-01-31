#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <base/MacAddress.h>


namespace DB
{

class DataTypeMacAddress : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = MacAddress;
    using ColumnType = ColumnVector<MacAddress>;
    static constexpr auto type_id = TypeIndex::MacAddress;

    const char * getFamilyName() const override { return "MacAddress"; }

    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override;

    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override;

    bool canBeUsedInBitOperations() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(MacAddress); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash &) const override {}

    SerializationPtr doGetDefaultSerialization() const override;
};

}

