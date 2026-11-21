#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Serialization wrapper for reading Map keys or values as a subcolumn (e.g. `map.keys`, `map.values`).
/// In the bucketed Map format, keys and values are distributed across multiple bucket sub-streams.
/// This class knows how to read the buckets info stream, deserialize each bucket independently
/// via the underlying keys/values serialization, and reassemble the results into a single
/// Array column. Serialization (writing) is not supported because subcolumns are read-only.
class SerializationMapKeysOrValues final : public SimpleTextSerialization
{
private:
    /// Serialization for the actual keys or values data within each bucket sub-stream.
    SerializationPtr keys_or_values_serialization;
    MergeTreeMapSerializationVersion serialization_version;

    SerializationMapKeysOrValues(
        const SerializationPtr & keys_or_values_serialization_,
        MergeTreeMapSerializationVersion serialization_version_);

public:
    static UInt128 getHash(const SerializationPtr & keys_or_values_serialization_, MergeTreeMapSerializationVersion serialization_version_);
    static SerializationPtr create(const SerializationPtr & keys_or_values_serialization_, MergeTreeMapSerializationVersion serialization_version_);

    bool supportsPooling() const override { return keys_or_values_serialization->supportsPooling(); }

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

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

private:
    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map keys/values subcolumn");
    }

};

}

