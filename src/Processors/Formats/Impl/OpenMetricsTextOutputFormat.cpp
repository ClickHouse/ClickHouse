#include <Processors/Formats/Impl/OpenMetricsTextOutputFormat.h>

#include <Processors/Formats/Impl/OpenMetricsText.h>

#include <algorithm>
#include <cmath>
#include <set>
#include <type_traits>
#include <utility>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <Formats/FormatFactory.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Processors/Port.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using OpenMetricsText::FORMAT_NAME;
using OpenMetricsText::isValidMetricName;
using OpenMetricsText::isValidLabelName;
using OpenMetricsText::isValidOpenMetricsType;
using OpenMetricsText::normalizeOpenMetricsType;
using OpenMetricsText::validateLabelValue;
using OpenMetricsText::writeQuotedLabelValue;
using OpenMetricsText::millisToSecondsString;

/// `tags` accepts either `Map(String, String)` or `Array(Tuple(String, String))`: the two share the
/// `ColumnArray(ColumnTuple(keys, values))` representation, so a `TimeSeries` `tags` column exported
/// via `SELECT * FROM ts` round-trips regardless of which spelling the source uses.
bool isTagsColumnType(const DataTypePtr & type)
{
    if (isMap(type))
    {
        const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
        return isStringOrFixedString(type_map->getKeyType()) && isStringOrFixedString(type_map->getValueType());
    }
    if (isArray(type))
    {
        const auto * type_array = assert_cast<const DataTypeArray *>(type.get());
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
        return type_tuple && type_tuple->getElements().size() == 2
            && isStringOrFixedString(type_tuple->getElement(0)) && isStringOrFixedString(type_tuple->getElement(1));
    }
    return false;
}

/// `time_series` is `Array(Tuple(DateTime64, Float64))`: the series' (timestamp, value) points,
/// matching the `TimeSeries` engine's `time_series` column and the input format's accepted type.
bool isTimeSeriesColumnType(const DataTypePtr & type)
{
    if (!isArray(type))
        return false;
    const auto * type_array = assert_cast<const DataTypeArray *>(type.get());
    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    return type_tuple && type_tuple->getElements().size() == 2
        && isDateTime64(type_tuple->getElement(0)) && WhichDataType(type_tuple->getElement(1)).isFloat64();
}

template <typename ResType>
void getColumnPos(const Block & header, const String & col_name, bool (*pred)(const DataTypePtr &), ResType & res)
{
    static_assert(std::is_same_v<ResType, size_t> || std::is_same_v<ResType, std::optional<size_t>>, "Illegal ResType");

    constexpr bool is_optional = std::is_same_v<ResType, std::optional<size_t>>;

    if (header.has(col_name, true))
    {
        res = header.getPositionByName(col_name);
        const auto & col = header.getByName(col_name);
        if (!pred(is_optional ? removeNullable(col.type) : col.type))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type '{}' of column '{}' for output format '{}'",
                col.type->getName(), col_name, FORMAT_NAME);
        }
    }
    else
    {
        if constexpr (is_optional)
            res = std::nullopt;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' is required for output format '{}'", col_name, FORMAT_NAME);
    }
}

void validateOpenMetricsMetricName(const String & name)
{
    if (!isValidMetricName(name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid metric name '{}' for output format '{}'",
            name, FORMAT_NAME);
}

void validateOpenMetricsLabelName(const String & name)
{
    if (!isValidLabelName(name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid label name '{}' for output format '{}'",
            name, FORMAT_NAME);
}

/// Convert a raw `DateTime64` value at `scale` decimal places to Prometheus-compatible milliseconds.
Int64 dateTime64ToMillis(Int64 raw, UInt32 scale)
{
    if (scale == 3)
        return raw;
    if (scale < 3)
    {
        Int64 mult = 1;
        for (UInt32 i = scale; i < 3; ++i)
            mult *= 10;
        return raw * mult;
    }
    Int64 div = 1;
    for (UInt32 i = 3; i < scale; ++i)
        div *= 10;
    return raw / div;  /// sub-millisecond precision is truncated toward zero
}

/// OpenMetrics `number` on the wire: finite values are serialized as ClickHouse Float64 text; the
/// special values use the canonical OpenMetrics spellings `NaN` / `+Inf` / `-Inf`.
String formatSampleValue(double value)
{
    if (std::isnan(value))
        return "NaN";
    if (std::isinf(value))
        return value < 0 ? "-Inf" : "+Inf";
    WriteBufferFromOwnString buf;
    writeFloatText(value, buf);
    return buf.str();
}

/// Reads the `tags` column (either `Map` or `Array(Tuple)`) for one row into an ordered key/value
/// list, validating label names and rejecting duplicate keys.
void extractTags(const IColumn & column, size_t row_num, std::vector<std::pair<String, String>> & out)
{
    const ColumnArray * col_array = nullptr;
    if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(&column))
        col_array = &col_map->getNestedColumn();
    else
        col_array = checkAndGetColumn<ColumnArray>(&column);
    if (!col_array)
        return;

    const auto & col_tuple = assert_cast<const ColumnTuple &>(col_array->getData());
    const IColumn & keys = col_tuple.getColumn(0);
    const IColumn & values = col_tuple.getColumn(1);
    const auto & offsets = col_array->getOffsets();
    const size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
    const size_t end = offsets[row_num];

    std::set<String> seen;
    for (size_t j = start; j < end; ++j)
    {
        String key(keys.getDataAt(j));
        validateOpenMetricsLabelName(key);
        if (!seen.insert(key).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Duplicate label name '{}' for output format '{}'",
                key, FORMAT_NAME);
        out.emplace_back(std::move(key), String(values.getDataAt(j)));
    }
}

/// Reads the `time_series` column for one row into an ordered list of (millisecond, value) points.
void extractPoints(const IColumn & column, size_t row_num, UInt32 scale, std::vector<std::pair<Int64, double>> & out)
{
    const auto & col_array = assert_cast<const ColumnArray &>(column);
    const auto & col_tuple = assert_cast<const ColumnTuple &>(col_array.getData());
    const IColumn & timestamps = col_tuple.getColumn(0);
    const IColumn & values = col_tuple.getColumn(1);
    const auto & offsets = col_array.getOffsets();
    const size_t start = row_num == 0 ? 0 : offsets[row_num - 1];
    const size_t end = offsets[row_num];

    for (size_t j = start; j < end; ++j)
        out.emplace_back(dateTime64ToMillis(timestamps.getInt(j), scale), values.getFloat64(j));
}

}

OpenMetricsTextOutputFormat::OpenMetricsTextOutputFormat(
    WriteBuffer & out_,
    SharedHeader header_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_)
    , format_settings(format_settings_)
{
    const Block & header = getPort(PortKind::Main).getHeader();

    getColumnPos(header, "metric_name", isStringOrFixedString, pos.metric_name);
    getColumnPos(header, "time_series", isTimeSeriesColumnType, pos.time_series);

    getColumnPos(header, "metric_family", isStringOrFixedString, pos.metric_family);
    getColumnPos(header, "help", isStringOrFixedString, pos.help);
    getColumnPos(header, "type", isStringOrFixedString, pos.type);
    getColumnPos(header, "unit", isStringOrFixedString, pos.unit);
    getColumnPos(header, "tags", isTagsColumnType, pos.tags);

    /// `getColumnPos` strips `Nullable` from optional columns before predicate validation, but `write`
    /// later casts `tags` straight to `ColumnMap` / `ColumnArray`. Accepting `Nullable(...)` here would
    /// silently drop labels for every row, so reject it explicitly.
    if (pos.tags.has_value() && header.getByName("tags").type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Illegal type '{}' of column 'tags' for output format '{}': Nullable is not supported",
            header.getByName("tags").type->getName(), FORMAT_NAME);

    const auto & ts_type = header.getByName("time_series").type;
    const auto & ts_tuple = assert_cast<const DataTypeTuple &>(*assert_cast<const DataTypeArray &>(*ts_type).getNestedType());
    timestamp_scale = assert_cast<const DataTypeDateTime64 &>(*ts_tuple.getElement(0)).getScale();
}

void OpenMetricsTextOutputFormat::flushCurrentFamily()
{
    if (!current_family.started)
    {
        current_family = {};
        return;
    }

    size_t total_points = 0;
    for (const auto & series : current_family.series)
        total_points += series.points.size();
    if (total_points == 0)
    {
        current_family = {};
        return;
    }

    /// OpenMetrics 1.0 family-metadata conformance, checked before any of this family's bytes are
    /// written so a rejected family fails up front rather than leaving a dangling header on the wire.
    if (!current_family.type.empty() && !isValidOpenMetricsType(current_family.type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Invalid OpenMetrics metric type '{}' for metric family '{}' in output format '{}' "
            "(expected one of unknown/gauge/counter/stateset/info/histogram/gaugehistogram/summary)",
            current_family.type, current_family.name, FORMAT_NAME);

    /// If a unit is specified, the family name must carry it as a suffix (OpenMetrics 1.0 UNIT rule).
    if (!current_family.unit.empty())
    {
        const String unit_suffix = "_" + current_family.unit;
        if (!current_family.name.ends_with(unit_suffix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "OpenMetrics unit '{}' requires metric family '{}' to end with '{}' for output format '{}'",
                current_family.unit, current_family.name, unit_suffix, FORMAT_NAME);
    }

    /// A counter's family name (used in `# TYPE`/`# HELP`/`# UNIT`) must not carry the `_total`
    /// suffix, and each counter sample's on-wire name must (the optional `_created` sample aside).
    if (current_family.type == "counter")
    {
        if (current_family.name.ends_with("_total"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "OpenMetrics counter family name '{}' must not carry the '_total' suffix "
                "(it belongs on the sample name) for output format '{}'",
                current_family.name, FORMAT_NAME);
        for (const auto & series : current_family.series)
            if (!series.metric_name.ends_with("_total") && !series.metric_name.ends_with("_created"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OpenMetrics counter sample '{}' must end with '_total' or '_created' for output format '{}'",
                    series.metric_name, FORMAT_NAME);
    }

    /// Validate every label value before emitting any bytes, so a malformed value (a tab or other
    /// control character) fails the whole family up front instead of throwing mid-stream.
    for (const auto & series : current_family.series)
        for (const auto & [label_name, label_value] : series.tags)
            validateLabelValue(label_value);

    const auto write_descriptor = [this](const char * marker, const String & value)
    {
        if (value.empty())
            return;
        writeCString(marker, out);
        writeString(current_family.name, out);
        writeChar(' ', out);
        writeString(value, out);
        writeChar('\n', out);
    };

    write_descriptor("# HELP ", current_family.help);
    write_descriptor("# TYPE ", current_family.type);
    write_descriptor("# UNIT ", current_family.unit);

    for (const auto & series : current_family.series)
    {
        for (const auto & [ms, value] : series.points)
        {
            writeString(series.metric_name, out);

            if (!series.tags.empty())
            {
                writeChar('{', out);
                bool is_first = true;
                for (const auto & [name, label_value] : series.tags)
                {
                    if (!is_first)
                        writeChar(',', out);
                    is_first = false;
                    writeString(name, out);
                    writeChar('=', out);
                    writeQuotedLabelValue(label_value, out);
                }
                writeChar('}', out);
            }

            writeChar(' ', out);
            writeString(formatSampleValue(value), out);
            writeChar(' ', out);
            writeString(millisToSecondsString(ms), out);
            writeChar('\n', out);
        }
    }

    current_family = {};
}

String OpenMetricsTextOutputFormat::getString(const Columns & columns, size_t row_num, size_t column_pos)
{
    WriteBufferFromOwnString tout;
    serializations[column_pos]->serializeText(*columns[column_pos], row_num, tout, format_settings);
    return tout.str();
}

void OpenMetricsTextOutputFormat::write(const Columns & columns, size_t row_num)
{
    row_write_in_progress = true;

    String metric_name = getString(columns, row_num, pos.metric_name);
    validateOpenMetricsMetricName(metric_name);

    String family;
    if (pos.metric_family.has_value() && !columns[*pos.metric_family]->isNullAt(row_num))
        family = getString(columns, row_num, *pos.metric_family);

    const String & key = family.empty() ? metric_name : family;
    if (!current_family.started || current_family.key != key)
    {
        flushCurrentFamily();
        current_family.started = true;
        current_family.key = key;
        current_family.name = key;
        if (!family.empty())
            validateOpenMetricsMetricName(current_family.name);
    }

    if (pos.help.has_value() && !columns[*pos.help]->isNullAt(row_num))
    {
        String help = getString(columns, row_num, *pos.help);
        std::replace(help.begin(), help.end(), '\n', ' ');
        if (!help.empty())
        {
            if (current_family.help.empty())
                current_family.help = std::move(help);
            else if (current_family.help != help)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Conflicting '# HELP' metadata for metric family '{}' in output format '{}'",
                    current_family.name, FORMAT_NAME);
        }
    }

    if (pos.type.has_value() && !columns[*pos.type]->isNullAt(row_num))
    {
        String type = normalizeOpenMetricsType(getString(columns, row_num, *pos.type));
        if (!type.empty())
        {
            if (current_family.type.empty())
                current_family.type = std::move(type);
            else if (current_family.type != type)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Conflicting '# TYPE' metadata for metric family '{}' in output format '{}'",
                    current_family.name, FORMAT_NAME);
        }
    }

    if (pos.unit.has_value() && !columns[*pos.unit]->isNullAt(row_num))
    {
        String unit = getString(columns, row_num, *pos.unit);
        std::replace(unit.begin(), unit.end(), '\n', ' ');
        if (!unit.empty())
        {
            if (current_family.unit.empty())
                current_family.unit = std::move(unit);
            else if (current_family.unit != unit)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Conflicting '# UNIT' metadata for metric family '{}' in output format '{}'",
                    current_family.name, FORMAT_NAME);
        }
    }

    Series series;
    series.metric_name = std::move(metric_name);
    if (pos.tags.has_value())
        extractTags(*columns[*pos.tags], row_num, series.tags);
    extractPoints(*columns[pos.time_series], row_num, timestamp_scale, series.points);
    current_family.series.push_back(std::move(series));

    row_write_in_progress = false;
}

void OpenMetricsTextOutputFormat::finalizeImpl()
{
    if (!row_write_in_progress)
    {
        flushCurrentFamily();
        writeCString("# EOF\n", out);
    }
}

void registerOutputFormatOpenMetrics(FormatFactory & factory);
void registerOutputFormatOpenMetrics(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<OpenMetricsTextOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.setContentType(FORMAT_NAME, "application/openmetrics-text; version=1.0.0; charset=utf-8");
    /// Each stream ends with `# EOF`; appending another exposition would make the file unreadable.
    factory.markFormatHasNoAppendSupport(FORMAT_NAME);
}

}
