#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <base/IPv4andIPv6.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>


namespace DB
{

template <typename IPv>
class DataTypeIP : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = IPv;
    using ColumnType = ColumnVector<IPv>;
    static constexpr auto type_id = TypeToTypeIndex<IPv>;

    const char * getFamilyName() const override { return TypeName<IPv>.data(); }
    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override { return IPv{}; }

    MutableColumnPtr createColumn() const override {return ColumnVector<IPv>::create();}

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override { return true; }
    bool isValueRepresentedByUnsignedInteger() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(IPv); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override { return std::make_shared<SerializationIP<IPv>>(); }
};

using DataTypeIPv4 = DataTypeIP<IPv4>;
using DataTypeIPv6 = DataTypeIP<IPv6>;

}
