#include "SerializationInterval.h"

#include <Columns/ColumnsNumber.h>
#include <IO/WriteBuffer.h>
#include <Parsers/Kusto/Formatters.h>
#include <base/arithmeticOverflow.h>


namespace DB
{
using ColumnInterval = DataTypeInterval::ColumnType;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

SerializationInterval::SerializationInterval(IntervalKind interval_kind_) : interval_kind(std::move(interval_kind_))
{
}

void SerializationInterval::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(
        static_cast<void (ISerialization::*)(Field &, ReadBuffer &, const FormatSettings &) const>(&ISerialization::deserializeBinary),
        settings.interval.output_format,
        field,
        istr,
        settings);
}

void SerializationInterval::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(
        static_cast<void (ISerialization::*)(IColumn &, ReadBuffer &, const FormatSettings &) const>(&ISerialization::deserializeBinary),
        settings.interval.output_format,
        column,
        istr,
        settings);
}

void SerializationInterval::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    dispatch(
        &ISerialization::deserializeBinaryBulk, FormatSettings::IntervalOutputFormat::Numeric, column, istr, rows_offset, limit, avg_value_size_hint);
}

void SerializationInterval::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    dispatch(&ISerialization::deserializeBinaryBulkStatePrefix, FormatSettings::IntervalOutputFormat::Numeric, settings, state, cache);
}


void SerializationInterval::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    dispatch(
        &ISerialization::deserializeBinaryBulkWithMultipleStreams,
        FormatSettings::IntervalOutputFormat::Numeric,
        column,
        rows_offset,
        limit,
        settings,
        state,
        cache);
}


void SerializationInterval::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeTextCSV, settings.interval.output_format, column, istr, settings);
}

void SerializationInterval::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeTextEscaped, settings.interval.output_format, column, istr, settings);
}

void SerializationInterval::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeTextJSON, settings.interval.output_format, column, istr, settings);
}

void SerializationInterval::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeTextQuoted, settings.interval.output_format, column, istr, settings);
}

void SerializationInterval::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeTextRaw, settings.interval.output_format, column, istr, settings);
}


void SerializationInterval::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::deserializeWholeText, settings.interval.output_format, column, istr, settings);
}

void SerializationInterval::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(
        static_cast<void (ISerialization::*)(const Field &, WriteBuffer &, const FormatSettings &) const>(&ISerialization::serializeBinary),
        settings.interval.output_format,
        field,
        ostr,
        settings);
}

void SerializationInterval::serializeBinary(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(
        static_cast<void (ISerialization::*)(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const>(
            &ISerialization::serializeBinary),
        settings.interval.output_format,
        column,
        row,
        ostr,
        settings);
}

void SerializationInterval::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    dispatch(&ISerialization::serializeBinaryBulk, FormatSettings::IntervalOutputFormat::Numeric, column, ostr, offset, limit);
}

void SerializationInterval::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(&ISerialization::serializeBinaryBulkStatePrefix, FormatSettings::IntervalOutputFormat::Numeric, column, settings, state);
}

void SerializationInterval::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(&ISerialization::serializeBinaryBulkStateSuffix, FormatSettings::IntervalOutputFormat::Numeric, settings, state);
}

void SerializationInterval::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(
        &ISerialization::serializeBinaryBulkWithMultipleStreams,
        FormatSettings::IntervalOutputFormat::Numeric,
        column,
        offset,
        limit,
        settings,
        state);
}

void SerializationInterval::serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeText, settings.interval.output_format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeTextCSV, settings.interval.output_format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextEscaped(
    const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeTextEscaped, settings.interval.output_format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeTextJSON, settings.interval.output_format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextQuoted(
    const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeTextQuoted, settings.interval.output_format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextRaw(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(&ISerialization::serializeTextRaw, settings.interval.output_format, column, row, ostr, settings);
}

/// Everything below is trash for the Kusto dialect:

void SerializationKustoInterval::serializeText(
    const IColumn & column, const size_t row, WriteBuffer & ostr, const FormatSettings &) const
{
    const auto * interval_column = checkAndGetColumn<ColumnInterval>(&column);
    if (!interval_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

    Int64 value = interval_column->getData()[row];
    Int64 ticks = 0;
    if (common::mulOverflow(kind.toAvgNanoseconds(), value, ticks))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Formatting an interval in Kusto dialect will overflow");
    ticks = ticks / 100;
    std::string interval_as_string = formatKQLTimespan(ticks);
    ostr.write(interval_as_string.c_str(), interval_as_string.length());
}

void SerializationKustoInterval::deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, const bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization in the Kusto dialect is not implemented");
}

}
