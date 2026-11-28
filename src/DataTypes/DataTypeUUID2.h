#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Core/UUID.h>


namespace DB
{

/** `UUID2` is a variant of the `UUID` data type with correct (lexicographic / textual) ordering.
  *
  * It shares the in-memory representation and column (`ColumnVector<UUID>`) with `UUID`, but the 128-bit value is
  * stored as a plain big-endian integer of the 16 canonical bytes, so that natural integer comparison of the
  * underlying value matches the order of the textual representation (and of the canonical byte order used by other
  * systems). See `UUIDHelpers::swapHalves` for the relation between the two representations.
  *
  * This fixes the historical defect of `UUID`, which sorts by the second half of the value.
  */
class DataTypeUUID2 final : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = UUID;
    using ColumnType = ColumnVector<UUID>;
    static constexpr auto type_id = TypeIndex::UUID2;

    const char * getFamilyName() const override { return "UUID2"; }

    TypeIndex getTypeId() const override { return type_id; }
    TypeIndex getColumnType() const override { return TypeIndex::UUID; }

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

    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override;
};

}
