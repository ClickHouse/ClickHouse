#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <base/IPv4andIPv6.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>
#include <Common/SipHash.h>


namespace DB
{

class DataTypeIPv4 : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = IPv4;
    using ColumnType = ColumnVector<IPv4>;
    static constexpr auto type_id = TypeToTypeIndex<IPv4>;

    const char * getFamilyName() const override { return TypeName<IPv4>.data(); }

    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override { return IPv4{}; }

    MutableColumnPtr createColumn() const override {return ColumnVector<IPv4>::create();}

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
    size_t getSizeOfValueInMemory() const override { return sizeof(IPv4); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash & hash) const override;

    SerializationPtr doGetDefaultSerialization() const override { return std::make_shared<SerializationIP<IPv4>>(); }
};

class DataTypeIPv6 : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = IPv6;
    using ColumnType = ColumnVector<IPv6>;
    static constexpr auto type_id = TypeToTypeIndex<IPv6>;

    const char * getFamilyName() const override { return TypeName<IPv6>.data(); }

    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override { return IPv6{}; }

    MutableColumnPtr createColumn() const override {return ColumnVector<IPv6>::create();}

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(IPv6); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash & hash) const override;

    SerializationPtr doGetDefaultSerialization() const override { return std::make_shared<SerializationIP<IPv6>>(); }
};


}
