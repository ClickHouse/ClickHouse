#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnVariant.h>
#include <Common/SipHash.h>
#include <optional>


namespace DB
{

/** Variant data type.
  * This type represents a union of other data types.
  * For example, type Variant(T1, T2, ..., TN) means that each row of this type
  * has a value of either type T1 or T2 or ... or TN or none of them (NULL value).
  * Nullable(...), LowCardinality(Nullable(...)) and Variant(...) types are not allowed
  * inside Variant type.
  * The order of nested types doesn't matter: Variant(T1, T2) = Variant(T2, T1).
  * To have global order of nested types we sort variants by type names on Variant creation.
  * The index of a variant in a sorted list is called global variant discriminator.
  */
class DataTypeVariant final : public IDataType
{
private:
    DataTypes variants;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeVariant(const DataTypes & variants_);

    TypeIndex getTypeId() const override { return TypeIndex::Variant; }
    const char * getFamilyName() const override { return "Variant"; }

    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool isComparable() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool textCanContainOnlyValidUTF8() const override;
    bool haveMaximumSizeOfValue() const override;
    bool hasDynamicSubcolumnsDeprecated() const override;
    size_t getMaximumSizeOfValueInMemory() const override;

    void updateHashImpl(SipHash & hash) const override;

    const DataTypePtr & getVariant(size_t i) const { return variants[i]; }
    const DataTypes & getVariants() const { return variants; }

    /// Check if Variant has provided type in the list of variants and return its discriminator.
    std::optional<ColumnVariant::Discriminator> tryGetVariantDiscriminator(const String & type_name) const;

    void forEachChild(const ChildCallback & callback) const override;

private:
    std::string doGetName() const override;
    std::string doGetPrettyName(size_t indent) const override;
    SerializationPtr doGetDefaultSerialization() const override;
};

/// Check if conversion from from_type to to_type is Variant extension
/// (both types are Variants and to_type contains all variants from from_type).
bool isVariantExtension(const DataTypePtr & from_type, const DataTypePtr & to_type);

}

