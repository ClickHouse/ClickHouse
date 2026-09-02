#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <base/unit.h>

namespace DB
{

class ColumnString;

struct DeserializeBinaryBulkStateStringWithoutSizeStream : public ISerialization::DeserializeBinaryBulkState
{
    /// Whether full string data is required during deserialization
    bool need_string_data = false;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override;
};

class SerializationString final : public ISerialization
{
private:
    explicit SerializationString(MergeTreeStringSerializationVersion version_ = MergeTreeStringSerializationVersion::SINGLE_STREAM);

public:
    /// Arbitrary guard against absurd sizes from corrupted input, large enough for any real string.
    static constexpr size_t MAX_STRING_SIZE = 16_GiB;

    static UInt128 getHash(MergeTreeStringSerializationVersion version_);
    static SerializationPtr create(MergeTreeStringSerializationVersion version_ = MergeTreeStringSerializationVersion::SINGLE_STREAM);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

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

    void deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

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
    void serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

private:
    MergeTreeStringSerializationVersion version;

    /// dispatch helpers for enumerateStreams
    void enumerateStreamsWithSize(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const;
    void enumerateStreamsWithoutSize(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const;

    /// dispatch helpers for serializeBinaryBulkWithMultipleStreams
    void serializeBinaryBulkWithSizeStream(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;
    void serializeBinaryBulkWithoutSizeStream(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// dispatch helpers for deserializeBinaryBulkWithMultipleStreams
    /// Reads the size/offset stream (branching on position_independent_encoding), appends the read
    /// offsets to column's offsets, and returns the number of data bytes to read next.
    /// Precondition: settings.path.back() == Substream::StringSizes.
    size_t deserializeStringOffsetsAndGetDataSize(
        ColumnString & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        SubstreamsCache * cache) const;
    void deserializeBinaryBulkWithSizeStream(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        SubstreamsCache * cache) const;
    void deserializeBinaryBulkWithoutSizeStream(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;
};

}
