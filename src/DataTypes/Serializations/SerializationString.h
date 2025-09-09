#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// SubstreamsCacheStringElement is used in ClickHouse to cache intermediate states for string serialization, especially
/// when dealing with .size substreams together with string data stream.
///
/// Purpose:
/// - Provides temporary storage of string data and size information between different serialization passes.
/// - Avoids re-parsing and allows consistent handling of partially read substreams.
///
/// Typical usage by different serializers:
/// - `SerializationString` uses {data, partial_data} and updates `data`
/// - `SerializationStringWithSizeStream` uses {data, size, skipped_bytes} and updates {data, partial_size}
/// - `SerializationStringSize` uses {data, size, partial_size} and updates `size` (and may update `skipped_bytes`)
/// - `SerializationStringInlineSize` uses {data, size} and updates `size` (and may update `partial_data`)
struct SubstreamsCacheStringElement : public ISerialization::ISubstreamsCacheElement
{
    explicit SubstreamsCacheStringElement(
        ColumnPtr data_,
        ColumnPtr size_ = nullptr,
        ColumnPtr partial_data_ = nullptr,
        ColumnPtr partial_size_ = nullptr,
        size_t skipped_bytes_ = 0)
        : data(std::move(data_))
        , size(std::move(size_))
        , partial_data(std::move(partial_data_))
        , partial_size(std::move(partial_size_))
        , skipped_bytes(skipped_bytes_)
    {
    }

    ColumnPtr data; /// The full string column
    ColumnPtr size; /// The column storing string sizes
    ColumnPtr partial_data; /// Temporary storage of string data starting from current offset
    ColumnPtr partial_size; /// Temporary storage for the size substream starting from current offset
    size_t skipped_bytes = 0; /// Tracks how many bytes were skipped, used for partial consumption and alignment logic
};

class SerializationString final : public ISerialization
{
public:
    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
};

}
