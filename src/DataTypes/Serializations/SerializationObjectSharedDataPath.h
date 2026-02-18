#pragma once

#include <DataTypes/Serializations/SerializationObjectPool.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>

namespace DB
{

/// Serialization of dynamic Object paths from shared data.
class SerializationObjectSharedDataPath final : public SerializationWrapper
{
private:
    SerializationObjectSharedDataPath(const SerializationPtr & nested_, SerializationObjectSharedData::SerializationVersion serialization_version_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const DataTypePtr & subcolumn_type_, size_t bucket);

public:
    static SerializationPtr create(const SerializationPtr & nested_, SerializationObjectSharedData::SerializationVersion serialization_version_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const DataTypePtr & subcolumn_type_, size_t bucket)
    {
        auto ptr = SerializationPtr(new SerializationObjectSharedDataPath(nested_, serialization_version_, path_, path_subcolumn_, dynamic_type_, subcolumn_type_, bucket));
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    ~SerializationObjectSharedDataPath() override;

    String getName() const override
    {
        return "ObjectSharedDataPath(" + nested_serialization->getName() + ", " + path + ", " + path_subcolumn + ", " + std::to_string(bucket) + ")";
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

private:
    SerializationObjectSharedData::SerializationVersion serialization_version;
    SerializationPtr serialization_map;
    String path;
    String path_subcolumn;
    DataTypePtr dynamic_type;
    DataTypePtr subcolumn_type;
    SerializationPtr dynamic_serialization;
    size_t bucket;
};

}
