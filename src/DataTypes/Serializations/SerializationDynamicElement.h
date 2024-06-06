#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{


/// Serialization for Dynamic element when we read it as a subcolumn.
class SerializationDynamicElement final : public SerializationWrapper
{
private:
    /// To be able to deserialize Dynamic element as a subcolumn
    /// we need its type name and global discriminator.
    String dynamic_element_name;

public:
    SerializationDynamicElement(const SerializationPtr & nested_, const String & dynamic_element_name_)
        : SerializationWrapper(nested_)
        , dynamic_element_name(dynamic_element_name_)
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
};

}
