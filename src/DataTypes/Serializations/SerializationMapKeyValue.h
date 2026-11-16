#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization wrapper for reading a single key's value from a Map column as a subcolumn
/// (e.g. `map['key']`). In the bucketed Map format, this class determines which bucket
/// the requested key belongs to (via `getBucketForKey`), reads only that single bucket,
/// and extracts the value for the matching key from the deserialized data.
/// This avoids reading all buckets when only one key is needed.
/// A key with a float zero is the exception: the other zero can be in another bucket,
/// so `read_whole_map` reads all buckets, as for the BASIC format.
/// Serialization (writing) is not supported because subcolumns are read-only.
class SerializationMapKeyValue final : public SerializationWrapper
{
private:
    SerializationMapKeyValue(
        const SerializationPtr & value_serialization_,
        const SerializationPtr & map_nested_serialization_,
        MergeTreeMapSerializationVersion serialization_version_,
        ColumnPtr key_,
        const DataTypePtr & nested_type_,
        bool read_whole_map_);

public:
    static SerializationPtr create(
        const SerializationPtr & value_serialization_,
        const SerializationPtr & map_nested_serialization_,
        MergeTreeMapSerializationVersion serialization_version_,
        ColumnPtr key_,
        const DataTypePtr & nested_type_,
        bool read_whole_map_);

    /// Does not support pooling because it stores runtime data (ColumnPtr key).
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
    /// The serialization of the nested Map structure Array(Tuple(key_type, value_type)),
    /// or of the whole Map when `read_whole_map` is set. Used internally for reading Map data from disk.
    SerializationPtr map_nested_serialization;
    MergeTreeMapSerializationVersion serialization_version;
    /// The key value to look up (e.g. the literal from `map['key']`).
    ColumnPtr key;
    /// The type read by `map_nested_serialization`: Array(Tuple(key_type, value_type)), or the Map
    /// when `read_whole_map` is set.
    DataTypePtr nested_type;
    /// Read all buckets instead of the bucket of the key.
    bool read_whole_map;
};

}
