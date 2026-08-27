#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <base/Version.h>
#include <DataTypes/Serializations/SerializationVersion.h>


namespace DB
{

/// See `Version` (`base/base/Version.h`) for the semantic model this type serializes and compares:
/// a 4-component numeric `major.minor.patch.build` version, not semver (no pre-release/build
/// metadata suffixes), with `1.2` equal to `1.2.0.0`.
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

    /// Unlike IPv4/IPv6, where bit operations express subnet masking, bitAnd/bitOr on a packed
    /// version number has no meaning, so bitwise functions reject the type rather than accept it.
    bool canBeUsedInBitOperations() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    /// NOTE: intentionally mirrors DataTypeIPv4 (not DataTypeIPv6, despite Version being 128-bit
    /// like IPv6) for isValueRepresentedByNumber/Integer/UnsignedInteger. A full audit (see PR
    /// review discussion) found no live bug from this: the one real-looking risk (Version compared
    /// against a bare integer literal) already fails safely with NO_COMMON_TYPE because
    /// TypeIndex::Version is not enumerated in getLeastSupertype.cpp's getNumericType(), unlike
    /// IPv4. Note that statistics and top-k skip-index eligibility gate on
    /// isValueRepresentedByNumber(), not on isValueRepresentedByInteger(), so they are unaffected by
    /// the Integer flag. What the Integer flag does decide is the Distributed sharding key:
    /// StorageDistributed.cpp accepts a sharding expression of this type at CREATE and only fails on
    /// the first INSERT, whereas false would reject it at CREATE. That is a small argument for
    /// false, deliberately deferred to a follow-up design decision, not part of this fix pass.
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
