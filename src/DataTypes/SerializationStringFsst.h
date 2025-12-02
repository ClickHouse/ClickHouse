#pragma once

#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_DATA;
}

class SerializationStringFsst final : public ISerialization
{
private:
    SerializationPtr nested;

public:
    explicit SerializationStringFsst(SerializationPtr _nested)
        : nested(_nested)
    {
    }

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeBinary(field, ostr, settings);
    }
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeBinary(field, istr, settings);
    }
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeBinary(column, row_num, ostr, settings);
    }
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeBinary(column, istr, settings);
    }

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

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeTextEscaped(column, row_num, ostr, settings);
    }
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeTextEscaped(column, istr, settings);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeTextQuoted(column, row_num, ostr, settings);
    }
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeTextQuoted(column, istr, settings);
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeTextCSV(column, row_num, ostr, settings);
    }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeTextQuoted(column, istr, settings);
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeText(column, row_num, ostr, settings);
    }
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeTextQuoted(column, istr, settings);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested->serializeTextJSON(column, row_num, ostr, settings);
    }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        nested->deserializeTextQuoted(column, istr, settings);
    }

private:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "text escaped/text quoted/text csv/whole text/json serialization are not implemented for FSST");
    }
};

}
