#pragma once

#include <DataTypes/IDataType.h>
#include <base/types.h>

constexpr size_t MAX_FIXEDSTRING_SIZE = 0xFFFFFF;
constexpr size_t MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS = 256;

namespace DB
{

class ColumnFixedString;

enum class FixedStringTextRepresentation : UInt8
{
    Raw,
    Hex,
    Base64,
    Base64URL,
    Base58,
};

String fixedStringTextRepresentationToString(FixedStringTextRepresentation representation);
FixedStringTextRepresentation parseFixedStringTextRepresentation(const String & representation);

class DataTypeFixedString final : public IDataType
{
private:
    size_t n;
    FixedStringTextRepresentation text_representation;

public:
    using ColumnType = ColumnFixedString;

    static constexpr bool is_parametric = true;
    static constexpr auto type_id = TypeIndex::FixedString;

    explicit DataTypeFixedString(size_t n_, FixedStringTextRepresentation text_representation_ = FixedStringTextRepresentation::Raw);

    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return type_id; }

    const char * getFamilyName() const override { return "FixedString"; }

    size_t getN() const
    {
        return n;
    }

    FixedStringTextRepresentation getTextRepresentation() const
    {
        return text_representation;
    }

    bool hasCustomTextRepresentation() const
    {
        return text_representation != FixedStringTextRepresentation::Raw;
    }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings &) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return n; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }

    void updateHashImpl(SipHash & hash) const override;
};

}
