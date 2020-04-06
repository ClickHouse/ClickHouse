#pragma once

#include <DataTypes/IDataTypeDummy.h>


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

    /// These methods read and write zero bytes just to allow to figure out size of column.
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return 0; }
    bool canBeInsideNullable() const override { return true; }
};

}
