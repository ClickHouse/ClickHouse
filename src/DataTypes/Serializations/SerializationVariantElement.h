#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnVariant.h>

namespace DB
{

class SerializationVariant;
class SerializationVariantElementNullMap;

/// Serialization for Variant element when we read it as a subcolumn.
class SerializationVariantElement final : public SerializationWrapper
{
private:
    /// To be able to deserialize Variant element as a subcolumn
    /// we need its type name and global discriminator.
    String variant_element_name;
    ColumnVariant::Discriminator variant_discriminator;
    /// Total number of variants in the Variant type; used for bounds-checking
    /// compact discriminators read from the wire.
    size_t num_variants;

    SerializationVariantElement(
        const SerializationPtr & nested_,
        const String & variant_element_name_,
        ColumnVariant::Discriminator variant_discriminator_,
        size_t num_variants_)
        : SerializationWrapper(nested_)
        , variant_element_name(variant_element_name_)
        , variant_discriminator(variant_discriminator_)
        , num_variants(num_variants_)
    {
    }

public:
    static UInt128 getHash(const SerializationPtr & nested_, const String & variant_element_name_, ColumnVariant::Discriminator variant_discriminator_, size_t num_variants_);
    static SerializationPtr create(
        const SerializationPtr & nested_,
        const String & variant_element_name_,
        ColumnVariant::Discriminator variant_discriminator_,
        size_t num_variants_ = 0);
    size_t allocatedBytes() const override;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    struct VariantSubcolumnCreator : public ISubcolumnCreator
    {
    private:
        const ColumnPtr local_discriminators;
        const ColumnPtr null_map; /// optional
        const String variant_element_name;
        const ColumnVariant::Discriminator global_variant_discriminator;
        const ColumnVariant::Discriminator local_variant_discriminator;
        bool make_nullable;
        size_t num_variants;

    public:
        VariantSubcolumnCreator(
            const ColumnPtr & local_discriminators_,
            const String & variant_element_name_,
            ColumnVariant::Discriminator global_variant_discriminator_,
            ColumnVariant::Discriminator local_variant_discriminator_,
            bool make_nullable_,
            const ColumnPtr & null_map_ = nullptr,
            size_t num_variants_ = 0);

        DataTypePtr create(const DataTypePtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override;
    };
private:
    friend SerializationVariant;
    friend SerializationVariantElementNullMap;

    struct DeserializeBinaryBulkStateVariantElement;

    static std::pair<size_t, size_t> deserializeCompactDiscriminators(
        ColumnPtr & discriminators_column,
        ColumnVariant::Discriminator variant_discriminator,
        size_t rows_offset,
        size_t limit,
        ReadBuffer * stream,
        bool continuous_reading,
        DeserializeBinaryBulkStatePtr & discriminators_state_,
        const DeserializeBinaryBulkSettings & settings,
        size_t num_variants,
        const ISerialization * serialization);

    void addVariantToPath(SubstreamPath & path) const;
    void removeVariantFromPath(SubstreamPath & path) const;
};

}
