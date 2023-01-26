#include "SerializationInterval.h"

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Parsers/Kusto/ParserKQLTimespan.h>

#include <magic_enum.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

using ColumnInterval = DataTypeInterval::ColumnType;

SerializationInterval::SerializationInterval(IntervalKind kind_)
    : SerializationCustomSimpleText(DataTypeFactory::instance().get("Int64")->getDefaultSerialization()), kind(std::move(kind_))
{
}

void SerializationInterval::serializeText(
    const IColumn & column, const size_t row_num, WriteBuffer & ostr, const FormatSettings & format_settings) const
{
    const auto * interval_column = checkAndGetColumn<ColumnInterval>(column);
    if (!interval_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected column of underlying type of Interval");

    if (const auto & format = format_settings.interval.format; format == FormatSettings::IntervalFormat::Numeric)
        nested_serialization->serializeText(column, row_num, ostr, format_settings);
    else if (format == FormatSettings::IntervalFormat::KQL)
    {
        const auto & value = interval_column->getData()[row_num];
        const auto ticks = kind.toAvgNanoseconds() * value / 100;
        const auto interval_as_string = ParserKQLTimespan::compose(ticks);
        ostr.write(interval_as_string.c_str(), interval_as_string.length());
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Option {} is not implemented", magic_enum::enum_name(format));
}

void SerializationInterval::deserializeText(
    [[maybe_unused]] IColumn & column,
    [[maybe_unused]] ReadBuffer & istr,
    [[maybe_unused]] const FormatSettings & format_settings,
    [[maybe_unused]] const bool whole) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization is not implemented for {}", kind.toNameOfFunctionToIntervalDataType());
}
}
