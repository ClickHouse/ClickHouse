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

/// Serialization wrapper for reading Map size as a subcolumn (e.g. `map.size0`).
/// In the bucketed Map format, array offsets are stored per bucket. The total map size
/// for each row is the sum of per-bucket sizes. This class reads offset sub-streams
/// from all buckets and sums them to produce the final UInt64 size column.
/// Serialization (writing) is not supported because subcolumns are read-only.
class SerializationMapSize final : public SimpleTextSerialization
{
private:
    /// Serialization for the array offsets sub-stream within each bucket.
    SerializationPtr size_serialization;
    MergeTreeMapSerializationVersion serialization_version;

    SerializationMapSize(
        const SerializationPtr & size_serialization_,
        MergeTreeMapSerializationVersion serialization_version_);

public:
    static UInt128 getHash(const SerializationPtr & size_serialization_, MergeTreeMapSerializationVersion serialization_version_);
    static SerializationPtr create(const SerializationPtr & size_serialization_, MergeTreeMapSerializationVersion serialization_version_);

    bool supportsPooling() const override { return size_serialization->supportsPooling(); }

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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map size subcolumn");
    }

};

}

