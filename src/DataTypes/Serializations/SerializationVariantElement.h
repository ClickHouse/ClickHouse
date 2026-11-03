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
    /// True when the requested subcolumn was wrapped into Nullable or LowCardinality(Nullable) by
    /// the extraction framework: that wrapper is not part of what nested_serialization serializes,
    /// so it must be removed before recursing into it. False when the requested type is
    /// intrinsically nullable, in which case nested_serialization requires that nullability.
    bool nullable_added_by_extraction;
    /// True when the subcolumn read through this element is a bare `UInt8` null map. It is the one
    /// subcolumn whose value for an absent element is not its default: the element is not there, so the
    /// extracted value is `NULL` and the map must read 1 rather than 0.
    bool selected_subcolumn_is_null_map;

    SerializationVariantElement(
        const SerializationPtr & nested_,
        const String & variant_element_name_,
        ColumnVariant::Discriminator variant_discriminator_,
        size_t num_variants_,
        bool nullable_added_by_extraction_,
        bool selected_subcolumn_is_null_map_)
        : SerializationWrapper(nested_)
        , variant_element_name(variant_element_name_)
        , variant_discriminator(variant_discriminator_)
        , num_variants(num_variants_)
        , nullable_added_by_extraction(nullable_added_by_extraction_)
        , selected_subcolumn_is_null_map(selected_subcolumn_is_null_map_)
    {
    }

    /// Appends `num_rows` rows for rows whose discriminator is not this element's.
    void insertRowsForAbsentElement(IColumn & inner_column, size_t num_rows) const;

public:
    static UInt128 getHash(const SerializationPtr & nested_, const String & variant_element_name_, ColumnVariant::Discriminator variant_discriminator_, size_t num_variants_, bool nullable_added_by_extraction_, bool selected_subcolumn_is_null_map_);
    static SerializationPtr create(
        const SerializationPtr & nested_,
        const String & variant_element_name_,
        ColumnVariant::Discriminator variant_discriminator_,
        size_t num_variants_,
        bool nullable_added_by_extraction_,
        bool selected_subcolumn_is_null_map_ = false);

    /// The null-map substreams `hasSubcolumnForPath` accepts. `NullMapHidden` is deliberately excluded
    /// there and so is not one of them.
    static bool isNullMapSubstream(Substream::Type type);
    size_t allocatedBytes() const override;
    MutableColumnPtr wrapColumnForDeserialization(MutableColumnPtr column) const override;

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
        IColumn & column,
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
        bool selected_subcolumn_is_null_map;

        /// Absence of the element goes into the selection itself only when it is a null map and the
        /// extraction did not wrap the element in `Nullable`, where absence is already expressed as `NULL`.
        /// The callers additionally require the selection to still be a bare `UInt8`.
        bool absenceGoesIntoNullMap() const { return selected_subcolumn_is_null_map && !make_nullable; }

    public:
        VariantSubcolumnCreator(
            const ColumnPtr & local_discriminators_,
            const String & variant_element_name_,
            ColumnVariant::Discriminator global_variant_discriminator_,
            ColumnVariant::Discriminator local_variant_discriminator_,
            bool make_nullable_,
            const ColumnPtr & null_map_ = nullptr,
            size_t num_variants_ = 0,
            bool selected_subcolumn_is_null_map_ = false);

        DataTypePtr create(const DataTypePtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override;
        std::shared_ptr<const ISubcolumnCreator> specializeForSelectedSubcolumn(const Substream & selected) const override;
    };
private:
    friend SerializationVariant;
    friend SerializationVariantElementNullMap;

    struct DeserializeBinaryBulkStateVariantElement;

    static size_t deserializeCompactDiscriminators(
        IColumn & discriminators_column,
        ColumnVariant::Discriminator variant_discriminator,
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
