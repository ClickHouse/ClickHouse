#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Core/UUID.h>
#include <Common/SipHash.h>


namespace DB
{

class DataTypeUUID : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = UUID;
    using ColumnType = ColumnVector<UUID>;
    static constexpr auto type_id = TypeIndex::UUID;

    const char * getFamilyName() const override { return "UUID"; }

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
    size_t getSizeOfValueInMemory() const override { return sizeof(UUID); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash &) const override {}

    SerializationPtr doGetDefaultSerialization() const override;
};

}
