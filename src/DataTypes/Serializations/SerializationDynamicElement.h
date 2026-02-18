#pragma once

#include <DataTypes/Serializations/SerializationObjectPool.h>
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
    /// Nested subcolumn of a type dynamic type. For example, for `Tuple(a UInt32)`.a
    /// subcolumn dynamic_element_name = 'Tuple(a UInt32)' and nested_subcolumn = 'a'.
    /// Needed to extract nested subcolumn from values in shared variant.
    String nested_subcolumn;
    bool is_null_map_subcolumn;

    SerializationDynamicElement(const SerializationPtr & nested_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_ = false)
        : SerializationWrapper(nested_), dynamic_element_name(dynamic_element_name_), nested_subcolumn(nested_subcolumn_), is_null_map_subcolumn(is_null_map_subcolumn_)
    {
    }

public:
    static SerializationPtr create(const SerializationPtr & nested_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_ = false)
    {
        auto ptr = SerializationPtr(new SerializationDynamicElement(nested_, dynamic_element_name_, nested_subcolumn_, is_null_map_subcolumn_));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    ~SerializationDynamicElement() override;

    String getName() const override
    {
        return "DynamicElement(" + nested_serialization->getName() + ", " + dynamic_element_name + ", " + nested_subcolumn + ", " + std::to_string(is_null_map_subcolumn) + ")";
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
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
};

}
