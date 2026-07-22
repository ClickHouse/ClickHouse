#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <base/Version.h>
#include <DataTypes/Serializations/SerializationVersion.h>


namespace DB
{

class DataTypeVersion final : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    using FieldType = Version;
    using ColumnType = ColumnVector<Version>;
    static constexpr auto type_id = TypeToTypeIndex<Version>;

    const char * getFamilyName() const override { return TypeName<Version>.data(); }

    TypeIndex getTypeId() const override { return type_id; }

    Field getDefault() const override { return Version{}; }

    MutableColumnPtr createColumn() const override {return ColumnVector<Version>::create();}

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }
    void updateHashImpl(SipHash &) const override {}

    /// NOTE: mirrors DataTypeIPv4 verbatim per plan (rule 5); bitwise AND/OR on a packed version
    /// number is semantically questionable but this flag only gates whether bitAnd/bitOr etc.
    /// accept the type, so we mirror IPv4 exactly rather than second-guess it (flagged in open_risks).
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
    size_t getSizeOfValueInMemory() const override { return sizeof(Version); }
    bool isCategorial() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override { return SerializationVersion::create(); }
};


}
