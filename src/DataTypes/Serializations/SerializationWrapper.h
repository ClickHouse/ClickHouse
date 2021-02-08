#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class SerializationWrapper : public ISerialization
{
protected:
    SerializationPtr nested;

public:
    SerializationWrapper(const SerializationPtr & nested_) : nested(nested_) {}

    void enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const override;

    void serializeBinaryBulkStatePrefix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const override;
      
    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
      
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
      
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
      
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
};

}
