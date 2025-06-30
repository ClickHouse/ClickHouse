#pragma once

#include <DataTypes/IDataTypeDummy.h>
#include <Common/SipHash.h>


namespace DB
{

/** Data type that cannot have any values.
  * Used to represent NULL of unknown type as Nullable(Nothing),
  * and possibly for empty array of unknown type as Array(Nothing).
  */
class DataTypeNothing final : public IDataTypeDummy
{
public:
    static constexpr bool is_parametric = false;

    const char * getFamilyName() const override { return "Nothing"; }

    TypeIndex getTypeId() const override { return TypeIndex::Nothing; }

    MutableColumnPtr createColumn() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return 0; }
    bool canBeInsideNullable() const override { return true; }
    bool isComparable() const override { return true; }

    void updateHashImpl(SipHash &) const override {}

    SerializationPtr doGetDefaultSerialization() const override;
};

}
