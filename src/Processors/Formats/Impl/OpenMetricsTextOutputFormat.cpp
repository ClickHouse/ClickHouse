#include <Processors/Formats/Impl/OpenMetricsTextOutputFormat.h>

#include <Processors/Formats/Impl/OpenMetricsText.h>

#include <algorithm>
#include <cmath>
#include <optional>
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
#include <DataTypes/DataTypeLowCardinality.h>
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
        return isStringOrFixedString(removeLowCardinality(type_map->getKeyType()))
            && isStringOrFixedString(removeLowCardinality(type_map->getValueType()));
    }
    if (isArray(type))
    {
        const auto * type_array = assert_cast<const DataTypeArray *>(type.get());
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
        return type_tuple && type_tuple->getElements().size() == 2
            && isStringOrFixedString(removeLowCardinality(type_tuple->getElement(0)))
            && isStringOrFixedString(removeLowCardinality(type_tuple->getElement(1)));
    }
    return false;
}

/// `samples` is `Array(Tuple(DateTime64, Float64))`: the series' (timestamp, value) points,
/// matching the `TimeSeries` engine's `samples` column and the input format's accepted type.
bool isSamplesColumnType(const DataTypePtr & type)
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
        /// `LowCardinality` is looked through: `SELECT * FROM <TimeSeries table>` returns `LowCardinality(String)`
        /// for columns read from `LowCardinality` inner columns.
        if (!pred(is_optional ? removeLowCardinalityAndNullable(col.type) : removeLowCardinality(col.type)))
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
/// list, validating label names and rejecting duplicate keys. The `__name__` tag is returned
/// separately in `name_tag`.
void extractTags(const IColumn & column, size_t row_num, std::vector<std::pair<String, String>> & out, std::optional<String> & name_tag)
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
        /// `TimeSeries` tables return the metric name as the `__name__` tag; it is the sample name, not a label.
        if (key == "__name__")
        {
            name_tag = String(values.getDataAt(j));
            continue;
        }
        validateOpenMetricsLabelName(key);
        if (!seen.insert(key).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Duplicate label name '{}' for output format '{}'",
                key, FORMAT_NAME);
        out.emplace_back(std::move(key), String(values.getDataAt(j)));
    }
}

/// Reads the `samples` column for one row into an ordered list of (millisecond, value) points.
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
    {
        const Int64 raw = timestamps.getInt(j);
        Int64 ms = 0;
        if (!OpenMetricsText::tryRescaleDateTime64(raw, scale, 3, ms))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Timestamp {} of DateTime64({}) does not fit Int64 milliseconds for output format '{}'", raw, scale, FORMAT_NAME);
        out.emplace_back(ms, values.getFloat64(j));
    }
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
    /// The points column is `samples`, or `time_series` as in `TimeSeries` tables of version 2 and earlier.
    const bool has_samples = header.has("samples", true);
    const bool has_time_series = header.has("time_series", true);
    if (has_samples && has_time_series)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Columns 'samples' and 'time_series' cannot both be present for output format '{}'", FORMAT_NAME);
    const String samples_column = has_time_series ? "time_series" : "samples";
    getColumnPos(header, samples_column, isSamplesColumnType, pos.samples);

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

    const auto & ts_type = header.getByPosition(pos.samples).type;
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
    /// suffix: it belongs on the sample name.
    if (current_family.type == "counter" && current_family.name.ends_with("_total"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "OpenMetrics counter family name '{}' must not carry the '_total' suffix "
            "(it belongs on the sample name) for output format '{}'",
            current_family.name, FORMAT_NAME);

    /// For the types with a suffix rule (counter, histogram, gaugehistogram, summary, info), each sample
    /// name must be the family name plus one of the type's suffixes, which is the same table the reader
    /// uses to fold samples back into their family. Histogram buckets must also carry `le`, and bare
    /// summary samples `quantile`.
    if (const auto * suffixes = OpenMetricsText::familySampleSuffixes(current_family.type))
    {
        for (const auto & series : current_family.series)
        {
            const std::string_view name = series.metric_name;
            const auto it = std::find_if(suffixes->begin(), suffixes->end(), [&](std::string_view suffix)
            {
                return name.size() == current_family.name.size() + suffix.size()
                    && name.starts_with(current_family.name) && name.ends_with(suffix);
            });
            if (it == suffixes->end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OpenMetrics {} sample '{}' is not a valid sample name of metric family '{}' for output format '{}'",
                    current_family.type, series.metric_name, current_family.name, FORMAT_NAME);

            if (const char * boundary = OpenMetricsText::requiredBoundaryLabel(current_family.type, *it))
            {
                const bool has_boundary = std::any_of(series.tags.begin(), series.tags.end(),
                    [&](const auto & tag) { return tag.first == boundary; });
                if (!has_boundary)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "OpenMetrics {} sample '{}' is missing the '{}' label for output format '{}'",
                        current_family.type, series.metric_name, boundary, FORMAT_NAME);
            }
        }
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

    Series series;
    series.metric_name = getString(columns, row_num, pos.metric_name);
    std::optional<String> name_tag;
    if (pos.tags.has_value())
        extractTags(*columns[*pos.tags], row_num, series.tags, name_tag);
    extractPoints(*columns[pos.samples], row_num, timestamp_scale, series.points);

    if (name_tag)
    {
        if (series.metric_name.empty())
            series.metric_name = std::move(*name_tag);
        else if (series.metric_name != *name_tag)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Metric name '{}' does not match the '__name__' tag '{}' for output format '{}'",
                series.metric_name, *name_tag, FORMAT_NAME);
    }

    String family;
    if (pos.metric_family.has_value() && !columns[*pos.metric_family]->isNullAt(row_num))
        family = getString(columns, row_num, *pos.metric_family);

    /// `SELECT * FROM <TimeSeries table>` also returns one row per metric family that carries only the
    /// family metadata: no metric name, no tags, and no points. Such a row contributes `# HELP` /
    /// `# TYPE` / `# UNIT` to its family but no series.
    const bool metadata_only = series.metric_name.empty() && series.tags.empty() && series.points.empty() && !family.empty();
    if (!metadata_only)
        validateOpenMetricsMetricName(series.metric_name);

    const String & key = family.empty() ? series.metric_name : family;
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

    if (!metadata_only)
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

    factory.setDocumentation(FORMAT_NAME, Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Reads and writes [OpenMetrics text](https://openmetrics.io/) (Prometheus-compatible exposition text with OpenMetrics extensions).

Use `FORMAT OpenMetrics` for input (`INSERT`, table functions such as `file` and `format`, and other read paths) and for query output (`SELECT ... FORMAT OpenMetrics`). The registered HTTP content type is `application/openmetrics-text; version=1.0.0; charset=utf-8`, and the writer produces [OpenMetrics 1.0](https://prometheus.io/docs/specs/om/open_metrics_spec/)-conformant exposition (see [OpenMetrics 1.0 conformance](#openmetrics-10-conformance)).

The columns are the outer columns of the [TimeSeries](/reference/engines/table-engines/integrations/time-series) table engine: one row is one time series (a `metric_name` and its `tags`), and its samples are collected into a `samples` array of `(timestamp, value)` points. A `TimeSeries` table can therefore be exported with `SELECT * ... FORMAT OpenMetrics` and filled with `INSERT INTO ... FORMAT OpenMetrics` (see [Exporting from and importing into a TimeSeries table](#timeseries-table)).

### Table shape {#table-shape}

| Column | Type | Role |
|--------|------|------|
| `metric_name` | [String](/reference/data-types/string) | On-wire sample name, including any suffix (`http_requests_total`, `foo_bucket`, `foo_count`). |
| `samples` | [Array](/reference/data-types/array)([Tuple](/reference/data-types/tuple)([DateTime64(3)](/reference/data-types/datetime64), [Float64](/reference/data-types/float))) | The series' `(timestamp, value)` points. The column may also be named `time_series`, as in `TimeSeries` tables of version 2 and earlier. |
| `metric_family` | [String](/reference/data-types/string) | Family name used in `# HELP`/`# TYPE`/`# UNIT` (`http_requests`, `foo`). Optional; defaults to `metric_name`. |
| `help` | [String](/reference/data-types/string) | `# HELP` text. Optional. |
| `type` | [String](/reference/data-types/string) | `# TYPE` value. Optional. |
| `unit` | [String](/reference/data-types/string) | `# UNIT` value. Optional. |
| `tags` | [Array](/reference/data-types/array)([Tuple](/reference/data-types/tuple)([String](/reference/data-types/string), [String](/reference/data-types/string))) or [Map(String, String)](/reference/data-types/map) | The series' labels. Optional. |

`metric_name` and `samples` are required on output; the inferred external schema (used by `file` and `format` when no structure is given) contains all seven columns, with `tags` inferred as `Array(Tuple(String, String))`. Both `tags` spellings are accepted on input and output. The `DateTime64` in `samples` may have any scale; the wire format carries millisecond precision.

The format reports `supports_subsets_of_columns = 1`, so a query that reads OpenMetrics text may project to any subset of the inferred columns (for example `SELECT metric_name FROM file(..., OpenMetrics)`). The parser still validates every sample line's grammar; it just skips inserting columns the query did not request.

### Row grouping {#row-grouping}

- **Input** buffers the whole exposition to `# EOF`, then emits one row per `(metric_name, tags)` series with all of that series' points collected into `samples`. Labels are sorted by name so a series' `tags` array is deterministic. `metric_family` is derived from the sample name and the declared `# TYPE` metadata (for example a `foo_bucket` sample under `# TYPE foo histogram` gets `metric_family = foo`).
- **Output** groups consecutive rows by `metric_family` (or by `metric_name` when `metric_family` is empty) to emit `# HELP`/`# TYPE`/`# UNIT` once per family, then one sample line per point. Rows must therefore be ordered so that each family is contiguous — add `ORDER BY metric_family` (or `metric_name`) to the query. Conflicting `help`/`type`/`unit` values within one family raise `BAD_ARGUMENTS`.
- On output, a `__name__` tag is not written as a label: it is the sample name, and it must match `metric_name` if both are set. A row with an empty `metric_name`, no tags and no points only provides the metadata of its `metric_family`; `SELECT * FROM <TimeSeries table>` returns such a row for every metric family.

### Timestamp handling {#timestamp-handling}

Point timestamps are carried as `DateTime64` inside the `samples` tuple. OpenMetrics text on the wire uses [`realnumber` epoch seconds](https://prometheus.io/docs/specs/om/open_metrics_spec/#abnf), so the format converts at the boundary:

- **Output:** the timestamp is emitted as `<seconds>.<3-digit-ms>` with trailing zeros stripped. For example `2018-03-12 15:53:27.789` → `1520879607.789`, a whole second → `1520879607`, `1970-01-01 00:00:00` → `0`. Digits finer than milliseconds are truncated toward zero.
- **Input:** the OpenMetrics token (any `realnumber`: integer, fractional, or with exponent) is converted to milliseconds with exact unsigned arithmetic, so values produced by `FORMAT OpenMetrics` round-trip without precision loss, and then to the scale of the target `DateTime64`. Fractional digits finer than the target precision are truncated toward zero. A sample line without an explicit timestamp is stored at epoch (`0`). Tokens whose value does not fit `Int64` at the target precision are rejected with `INCORRECT_DATA`.

### OpenMetrics 1.0 conformance {#openmetrics-10-conformance}

The writer enforces the [OpenMetrics 1.0](https://prometheus.io/docs/specs/om/open_metrics_spec/) structural rules, raising `BAD_ARGUMENTS` when a row cannot be represented conformantly:

- **Metric type vocabulary.** `type` must be one of `unknown`, `gauge`, `counter`, `stateset`, `info`, `histogram`, `gaugehistogram`, `summary`, or empty. The Prometheus spelling `untyped` is normalized to `unknown` on both input and output.
- **Sample names.** For the types with a suffix rule, each sample name must be the family name plus one of the type's suffixes: `_total` or `_created` for `counter`; `_bucket`, `_count`, `_sum`, or `_created` for `histogram`; `_bucket`, `_gcount`, `_gsum`, or `_created` for `gaugehistogram`; no suffix, `_count`, `_sum`, or `_created` for `summary`; `_info` for `info`. A `counter` family name must not end with `_total`: the suffix lives on `metric_name`, never on `metric_family`. Histogram `_bucket` samples must carry an `le` label, and bare `summary` samples a `quantile` label.
- **Unit suffix rule.** When `unit` is set, `metric_family` must end with `_<unit>`.
- **Exposition structure.** `# HELP`/`# TYPE`/`# UNIT` are emitted at most once per family; there are no blank lines between families; the stream ends with `# EOF`.

The reader accepts Prometheus-style exposition: it normalizes `untyped` → `unknown` and does not require the counter `_total` or unit suffix contract on input.

### Compared to `FORMAT Prometheus` {#compared-to-prometheus}

| Topic | `Prometheus` | `OpenMetrics` |
|-------|----------------|---------------|
| Input | Not supported | Supported |
| Row model | One row per sample | One row per series (`samples` array of points) |
| `# UNIT` lines | Not emitted | Emitted when `unit` is set |
| End of stream | N/A | Output ends with `# EOF`; input rejects non-whitespace after `# EOF` |
| Timestamp on the wire | Milliseconds | Epoch seconds with sub-second precision |
| OpenMetrics 1.0 | No | Yes (writer) |

### Input validation {#input-validation}

Malformed exposition raises `INCORRECT_DATA`, including duplicate label keys, invalid or empty metric/label names (also in `# HELP`, `# TYPE`, and `# UNIT` lines), a `# TYPE` that is not in the metric type vocabulary, raw control characters inside a quoted label value, whitespace between a metric name and its `{labels}` block, a missing ASCII space or tab between the metric descriptor and sample value, float tokens that are not fully consumed, invalid timestamp or exemplar tokens (OpenMetrics `realnumber` grammar), timestamps whose value overflows the target `Int64`, trailing characters after the value or timestamp on a sample line, invalid exemplar syntax after `#`, a histogram `_bucket` sample without an `le` label, a duplicate `# HELP`/`# TYPE`/`# UNIT` for the same family, a descriptor that follows the family's samples, a `# EOF` line with extra non-whitespace on the same line, and any non-blank content after a valid `# EOF` line.

On **output**, label values are quoted with OpenMetrics-specific escaping (`\\`, `\"`, and `\n` only); other control characters in a label value raise `BAD_ARGUMENTS`, as do duplicate keys within one series' `tags` and invalid metric/label names.

## Example usage {#example-usage}

### Reading OpenMetrics text {#reading-openmetrics-text}

```sql
SELECT *
FROM format(OpenMetrics, $$
# HELP http_requests Total number of HTTP requests
# TYPE http_requests counter
http_requests_total{code="200",method="POST"} 1027 1704067200
http_requests_total{code="200",method="GET"} 34 1704067200
# EOF
$$)
FORMAT Vertical;
```

Each `(metric_name, tags)` series becomes one row, with its samples collected into `samples`.

### Exporting from and importing into a TimeSeries table {#timeseries-table}

```sql
CREATE TABLE http_metrics ENGINE = TimeSeries;

INSERT INTO http_metrics (metric_name, tags, samples, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'POST', 'code': '200'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 1027)],
     'http_requests', 'counter', '', 'Total number of HTTP requests');

SELECT * FROM http_metrics
ORDER BY metric_family, metric_name
FORMAT OpenMetrics;
```

```text
# HELP http_requests Total number of HTTP requests
# TYPE http_requests counter
http_requests_total{code="200",method="POST"} 1027 1704067200
# EOF
```

Order by `metric_family` so that each family's descriptors are emitted once. The exposition can be inserted back into a `TimeSeries` table with `INSERT INTO http_metrics FORMAT OpenMetrics`.
)DOCS_MD"});
}

}
