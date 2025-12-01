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
    explicit SerializationStringFsst(SerializationPtr _nested) : nested(_nested) {}

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override;
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override;

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

    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNotImplemented(); }
    void deserializeTextEscaped(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNotImplemented(); }

    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNotImplemented(); }
    void deserializeTextQuoted(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNotImplemented(); }

    void serializeTextCSV(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNotImplemented(); }
    void deserializeTextCSV(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNotImplemented(); }

    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNotImplemented(); }
    void deserializeWholeText(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNotImplemented(); }

    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNotImplemented(); }
    void deserializeTextJSON(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNotImplemented(); }

private:
    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "text escaped/text quoted/text csv/whole text/json serialization are not implemented for FSST");
    }
};

}
