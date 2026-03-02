#pragma once

#include <Common/SipHash.h>
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
        return SerializationObjectPool::instance().getOrCreate(ptr->getHash(), std::move(ptr));
    }

    UInt128 getHash() const override
    {
        SipHash hash;
        hash.update("ObjectSharedDataPath");
        hash.update(nested_serialization->getHash());
        hash.update(static_cast<int>(serialization_version.value));
        hash.update(path);
        hash.update(path_subcolumn);
        hash.update(dynamic_type->getName());
        hash.update(subcolumn_type->getName());
        hash.update(bucket);
        return hash.get128();
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
