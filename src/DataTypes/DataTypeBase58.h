#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Core/Base58.h>


namespace DB
{

class DataTypeBase58 : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = Base58;
    using ColumnType = ColumnVector<Base58>;
    static constexpr auto type_id = TypeIndex::Base58;

    const char * getFamilyName() const override { return "Base58"; }
    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override;

    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override;

    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(Base58); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
