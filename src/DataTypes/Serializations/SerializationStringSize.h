#pragma once

#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

/// Enables the `.size` subcolumn for string columns.
class SerializationStringSize final : public SimpleTextSerialization
{
public:
    /// If true, the `.size` subcolumn is a real substream (new serialization).
    /// If false, it is a virtual subcolumn derived from the data (old serialization).
    explicit SerializationStringSize(bool with_size_stream_);

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

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

    void deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override;
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override;
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override;
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override;
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override;
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override;
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override;

private:
    /// Indicates whether `.size` is a real substream (true) or virtual (false).
    bool with_size_stream;

    /// Helper to access base string serialization logic.
    SerializationString serialization_string;

    /// dispatch helpers for deserializeBinaryBulkWithMultipleStreams
    void deserializeBinaryBulkWithSizeStream(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;
    void deserializeBinaryBulkWithoutSizeStream(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;
};

}
