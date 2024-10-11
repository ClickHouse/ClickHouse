#pragma once

#include "ISerialization.h"
#include "SerializationCustomSimpleText.h"

#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Common/IntervalKind.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SerializationKustoInterval : public SerializationCustomSimpleText
{
public:
    explicit SerializationKustoInterval(IntervalKind kind_) : SerializationCustomSimpleText(nullptr), kind(kind_) { }

    void serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

private:
    IntervalKind kind;
};

class SerializationInterval : public ISerialization
{
public:
    explicit SerializationInterval(IntervalKind kind_);

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;
    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;
    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;
    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;
    void serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextEscaped(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextQuoted(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextRaw(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    template <typename... Args, std::invocable<const ISerialization *, Args...> Method>
    void dispatch(const Method method, const FormatSettings::IntervalOutputFormat format, Args &&... args) const
    {
        const ISerialization * serialization = nullptr;
        if (format == FormatSettings::IntervalOutputFormat::Kusto)
            serialization = &serialization_kusto;
        else if (format == FormatSettings::IntervalOutputFormat::Numeric)
            serialization = &serialization_numeric;

        if (!serialization)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option {} is not implemented", magic_enum::enum_name(format));

        (serialization->*method)(std::forward<Args>(args)...);
    }

    IntervalKind interval_kind;
    SerializationKustoInterval serialization_kusto{interval_kind};
    SerializationNumber<typename DataTypeInterval::FieldType> serialization_numeric;
};
}
