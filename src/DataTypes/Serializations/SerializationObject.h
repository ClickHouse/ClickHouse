#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

template <typename Parser>
class SerializationObject : public ISerialization
{
public:
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

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

private:
    template <typename Settings, typename StatePtr>
    void checkSerializationIsSupported(Settings & settings, StatePtr & state) const;

    template <typename Reader>
    void deserializeTextImpl(IColumn & column, Reader && reader) const;

    mutable Parser parser;
};

SerializationPtr getObjectSerialization(const String & schema_format);

}
