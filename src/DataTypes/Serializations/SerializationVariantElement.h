#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
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

public:
    SerializationVariantElement(const SerializationPtr & nested_, const String & variant_element_name_, ColumnVariant::Discriminator variant_discriminator_)
        : SerializationWrapper(nested_)
        , variant_element_name(variant_element_name_)
        , variant_discriminator(variant_discriminator_)
    {
    }

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

    public:
        VariantSubcolumnCreator(
            const ColumnPtr & local_discriminators_,
            const String & variant_element_name_,
            ColumnVariant::Discriminator global_variant_discriminator_,
            ColumnVariant::Discriminator local_variant_discriminator_,
            bool make_nullable_,
            const ColumnPtr & null_map_ = nullptr);

        DataTypePtr create(const DataTypePtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev) const override;
    };
private:
    friend SerializationVariant;
    friend SerializationVariantElementNullMap;

    struct DeserializeBinaryBulkStateVariantElement;

    static size_t deserializeCompactDiscriminators(
        ColumnPtr & discriminators_column,
        ColumnVariant::Discriminator variant_discriminator,
        size_t limit,
        ReadBuffer * stream,
        bool continuous_reading,
        DeserializeBinaryBulkStatePtr & discriminators_state_,
        const ISerialization * serialization);

    void addVariantToPath(SubstreamPath & path) const;
    void removeVariantFromPath(SubstreamPath & path) const;
};

}
