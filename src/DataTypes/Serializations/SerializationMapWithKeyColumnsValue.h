#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Reads one key's dense value stream from a with_key_columns Map part.
class SerializationMapWithKeyColumnsValue final : public SerializationWrapper
{
public:
    static SerializationPtr create(
        const SerializationPtr & value_serialization_,
        const DataTypePtr & value_type_,
        const SerializationPtr & map_with_key_columns_serialization_,
        Field key_);

    /// Writes only this key's value streams under the parent Map column name.
    /// Does not touch `keys_info` (written by the presence pass).
    static SerializationPtr createForWrite(
        const SerializationPtr & value_serialization_,
        const DataTypePtr & value_type_,
        const SerializationPtr & map_with_key_columns_serialization_,
        Field key_);

    bool supportsPooling() const override { return false; }

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

private:
    SerializationMapWithKeyColumnsValue(
        const SerializationPtr & value_serialization_,
        const DataTypePtr & value_type_,
        const SerializationPtr & map_with_key_columns_serialization_,
        Field key_,
        bool write_value_only_);

    DataTypePtr value_type;
    SerializationPtr map_with_key_columns_serialization;
    Field key;
    String key_name;
    bool write_value_only = false;
};

}
