#pragma once

#include <memory>

#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_DATA;
}

struct CompressedField
{
    Field value;
    size_t uncompressed_size;
};

class SerializationStringFsst final : public ISerialization
{
public:
    explicit SerializationStringFsst(SerializationPtr _nested)
        : nested(_nested)
    {
    }

    KindStack getKindStack() const override;

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
    void serializeState(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const;
    size_t deserializeState(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const;

    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr nested;

        explicit SubcolumnCreator(const ColumnPtr & _nested)
            : nested(_nested)
        {
        }

        DataTypePtr create(const DataTypePtr & prev) const override { return prev; }
        SerializationPtr create(const SerializationPtr & nested, const DataTypePtr &) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };

    SerializationPtr nested;
    constexpr static size_t kCompressSize = 16 << 10; // 16KB
};

}
