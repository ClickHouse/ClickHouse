#include "SerializationInterval.h"

#include "SerializationCustomSimpleText.h"

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

#include <magic_enum.hpp>

#include <concepts>
#include <span>

namespace DB
{
using ColumnInterval = DataTypeInterval::ColumnType;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}
}

namespace
{
class SerializationKQLInterval : public DB::SerializationCustomSimpleText
{
public:
    explicit SerializationKQLInterval(DB::IntervalKind kind_) : SerializationCustomSimpleText(nullptr), kind(kind_) { }

    void serializeText(const DB::IColumn & column, size_t row, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const override;
    void deserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const override;

private:
    DB::IntervalKind kind;
};

void SerializationKQLInterval::serializeText(
    const DB::IColumn & column, const size_t row, DB::WriteBuffer & ostr, const DB::FormatSettings &) const
{
    const auto * interval_column = checkAndGetColumn<DB::ColumnInterval>(column);
    if (!interval_column)
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

    const auto & value = interval_column->getData()[row];
    const auto ticks = kind.toAvgNanoseconds() * value / 100;
    const auto interval_as_string = DB::ParserKQLTimespan::compose(ticks);
    ostr.write(interval_as_string.c_str(), interval_as_string.length());
}

void SerializationKQLInterval::deserializeText(
    [[maybe_unused]] DB::IColumn & column,
    [[maybe_unused]] DB::ReadBuffer & istr,
    [[maybe_unused]] const DB::FormatSettings & settings,
    [[maybe_unused]] const bool whole) const
{
    throw DB::Exception(
        DB::ErrorCodes::NOT_IMPLEMENTED, "Deserialization is not implemented for {}", kind.toNameOfFunctionToIntervalDataType());
}

template <typename... Args, std::invocable<const DB::ISerialization *, Args...> Method>
void dispatch(
    std::span<const std::unique_ptr<const DB::ISerialization>> serializations,
    const Method method,
    const DB::FormatSettings::IntervalFormat format,
    Args &&... args)
{
    const auto format_index = magic_enum::enum_index(format);
    if (!format_index)
        throw DB::Exception(DB::ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "No such format exists");

    const auto & serialization = serializations[*format_index];
    if (!serialization)
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Option {} is not implemented", magic_enum::enum_name(format));

    (serialization.get()->*method)(std::forward<Args>(args)...);
}
}

namespace DB
{
SerializationInterval::SerializationInterval(IntervalKind kind_)
{
    serializations.at(magic_enum::enum_index(FormatSettings::IntervalFormat::KQL).value())
        = std::make_unique<SerializationKQLInterval>(std::move(kind_));
    serializations.at(magic_enum::enum_index(FormatSettings::IntervalFormat::Numeric).value())
        = std::make_unique<SerializationNumber<typename DataTypeInterval::FieldType>>();
}

void SerializationInterval::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(
        serializations,
        static_cast<void (ISerialization::*)(Field &, ReadBuffer &, const FormatSettings &) const>(&ISerialization::deserializeBinary),
        settings.interval.format,
        field,
        istr,
        settings);
}

void SerializationInterval::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(
        serializations,
        static_cast<void (ISerialization::*)(IColumn &, ReadBuffer &, const FormatSettings &) const>(&ISerialization::deserializeBinary),
        settings.interval.format,
        column,
        istr,
        settings);
}

void SerializationInterval::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    dispatch(
        serializations,
        &ISerialization::deserializeBinaryBulk,
        FormatSettings::IntervalFormat::Numeric,
        column,
        istr,
        limit,
        avg_value_size_hint);
}

void SerializationInterval::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const
{
    dispatch(serializations, &ISerialization::deserializeBinaryBulkStatePrefix, FormatSettings::IntervalFormat::Numeric, settings, state);
}


void SerializationInterval::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    dispatch(
        serializations,
        &ISerialization::deserializeBinaryBulkWithMultipleStreams,
        FormatSettings::IntervalFormat::Numeric,
        column,
        limit,
        settings,
        state,
        cache);
}


void SerializationInterval::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeTextCSV, settings.interval.format, column, istr, settings);
}

void SerializationInterval::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeTextEscaped, settings.interval.format, column, istr, settings);
}

void SerializationInterval::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeTextJSON, settings.interval.format, column, istr, settings);
}

void SerializationInterval::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeTextQuoted, settings.interval.format, column, istr, settings);
}

void SerializationInterval::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeTextRaw, settings.interval.format, column, istr, settings);
}


void SerializationInterval::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::deserializeWholeText, settings.interval.format, column, istr, settings);
}

void SerializationInterval::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(
        serializations,
        static_cast<void (ISerialization::*)(const Field &, WriteBuffer &, const FormatSettings &) const>(&ISerialization::serializeBinary),
        settings.interval.format,
        field,
        ostr,
        settings);
}

void SerializationInterval::serializeBinary(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(
        serializations,
        static_cast<void (ISerialization::*)(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const>(
            &ISerialization::serializeBinary),
        settings.interval.format,
        column,
        row,
        ostr,
        settings);
}

void SerializationInterval::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    dispatch(serializations, &ISerialization::serializeBinaryBulk, FormatSettings::IntervalFormat::Numeric, column, ostr, offset, limit);
}

void SerializationInterval::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(
        serializations, &ISerialization::serializeBinaryBulkStatePrefix, FormatSettings::IntervalFormat::Numeric, column, settings, state);
}

void SerializationInterval::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(serializations, &ISerialization::serializeBinaryBulkStateSuffix, FormatSettings::IntervalFormat::Numeric, settings, state);
}

void SerializationInterval::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    dispatch(
        serializations,
        &ISerialization::serializeBinaryBulkWithMultipleStreams,
        FormatSettings::IntervalFormat::Numeric,
        column,
        offset,
        limit,
        settings,
        state);
}

void SerializationInterval::serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeText, settings.interval.format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeTextCSV, settings.interval.format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextEscaped(
    const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeTextEscaped, settings.interval.format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeTextJSON, settings.interval.format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextQuoted(
    const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeTextQuoted, settings.interval.format, column, row, ostr, settings);
}

void SerializationInterval::serializeTextRaw(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const
{
    dispatch(serializations, &ISerialization::serializeTextRaw, settings.interval.format, column, row, ostr, settings);
}
}
