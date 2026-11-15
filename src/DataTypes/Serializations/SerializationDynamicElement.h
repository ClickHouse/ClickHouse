#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization for Dynamic element when we read it as a subcolumn.
class SerializationDynamicElement final : public SerializationWrapper
{
private:
    /// Serialization of SharedVariant. It's used if the requested type is not serialized as separate variant.
    SerializationPtr shared_variant_serialization;
    /// To be able to deserialize Dynamic element as a subcolumn
    /// we need its type name and global discriminator.
    String dynamic_element_name;
    /// Nested subcolumn of a type dynamic type. For example, for `Tuple(a UInt32)`.a
    /// subcolumn dynamic_element_name = 'Tuple(a UInt32)' and nested_subcolumn = 'a'.
    /// Needed to extract nested subcolumn from values in shared variant.
    String nested_subcolumn;
    bool is_null_map_subcolumn;
    /// True when the extraction wrapped the requested subcolumn into Nullable or
    /// LowCardinality(Nullable). Only forwarded to the variant element serialization of the requested
    /// type; see SerializationVariantElement::nullable_added_by_extraction.
    bool nullable_added_by_extraction;
    /// True when `nested_subcolumn` is a bare `UInt8` null map of the requested type, e.g. `Tuple(a
    /// Nullable(UInt32))`.a.null. Unlike `is_null_map_subcolumn`, which is the null map OF the element,
    /// this one is read through the element; see `SerializationVariantElement`.
    bool selected_subcolumn_is_null_map;

    SerializationDynamicElement(const SerializationPtr & nested_, const SerializationPtr & shared_variant_serialization_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_, bool nullable_added_by_extraction_, bool selected_subcolumn_is_null_map_)
        : SerializationWrapper(nested_), shared_variant_serialization(shared_variant_serialization_), dynamic_element_name(dynamic_element_name_), nested_subcolumn(nested_subcolumn_), is_null_map_subcolumn(is_null_map_subcolumn_), nullable_added_by_extraction(nullable_added_by_extraction_), selected_subcolumn_is_null_map(selected_subcolumn_is_null_map_)
    {
    }

public:
    static UInt128 getHash(const SerializationPtr & nested_, const SerializationPtr & shared_variant_serialization_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_, bool nullable_added_by_extraction_, bool selected_subcolumn_is_null_map_);
    static SerializationPtr create(const SerializationPtr & nested_, const SerializationPtr & shared_variant_serialization_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_, bool nullable_added_by_extraction_, bool selected_subcolumn_is_null_map_ = false);
    size_t allocatedBytes() const override;
    bool supportsPooling() const override { return SerializationWrapper::supportsPooling() && shared_variant_serialization->supportsPooling(); }
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
};

}
