#include <Processors/Formats/Impl/OpenMetricsTextOutputFormat.h>

#include <Processors/Formats/Impl/OpenMetricsText.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <optional>
#include <type_traits>
#include <utility>

#include <base/sort.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>

#include <Common/assert_cast.h>

#include <Core/Field.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <Formats/FormatFactory.h>

#include <IO/ReadBufferFromString.h>
#include <IO/readFloatText.h>
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
using OpenMetricsText::isEmptyMarker;
using OpenMetricsText::sampleKindCount;
using OpenMetricsText::hasReservedMarkerLabelWithValue;
using OpenMetricsText::histogramSeriesLabels;
using OpenMetricsText::checkBoundaryLabel;
using OpenMetricsText::validateLabelValue;
using OpenMetricsText::writeQuotedLabelValue;
using OpenMetricsText::millisToSecondsString;

bool isDataTypeMapString(const DataTypePtr & type)
{
    if (!isMap(type))
        return false;
    const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
    return isStringOrFixedString(type_map->getKeyType()) && isStringOrFixedString(type_map->getValueType());
}

/// OpenMetrics timestamps must round-trip with the input parser, which stores into `Int64`
/// (or `Nullable(Int64)`) milliseconds; reject other numeric types to keep the contract explicit
/// and avoid silent precision drift through `Float64` or smaller integer widths.
bool isInt64OrNullableInt64(const DataTypePtr & type)
{
    const DataTypePtr & nested = type->isNullable()
        ? assert_cast<const DataTypeNullable *>(type.get())->getNestedType()
        : type;
    return WhichDataType(nested).isInt64();
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

Float64 tryParseFloat(const String & s)
{
    Float64 t = 0;
    ReadBufferFromString buf(s);
    tryReadFloatText(t, buf);
    return t;
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

/// The `type` value is emitted raw in the `# TYPE <name> <type>` family-metadata line. The reader
/// parses the type as a single whitespace-delimited token and rejects trailing data, and a newline
/// would inject extra lines, so a type carrying whitespace or control characters would produce a
/// stream this writer's own reader rejects. Reject those up front (documented free tokens such as
/// `untyped` / `unknown` still pass through verbatim).
void validateOpenMetricsType(const String & type)
{
    for (char c : type)
        if (c == ' ' || c == '\t' || static_cast<unsigned char>(c) < 32)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid type '{}' for output format '{}': it must be a single token without whitespace or control characters",
                type, FORMAT_NAME);
}

template <typename Container>
void columnMapToContainer(const ColumnMap * col_map, size_t row_num, Container & result)
{
    Field field;
    col_map->get(row_num, field);
    const auto & map_field = field.safeGet<Map>();
    for (const auto & map_element : map_field)
    {
        const auto & map_entry = map_element.safeGet<Tuple>();

        String entry_key;
        String entry_value;
        if (map_entry.size() == 2
            && map_entry[0].tryGet<String>(entry_key)
            && map_entry[1].tryGet<String>(entry_value))
        {
            validateOpenMetricsLabelName(entry_key);
            const auto [it, inserted] = result.emplace(entry_key, entry_value);
            if (!inserted)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Duplicate label name '{}' for output format '{}'",
                    it->first, FORMAT_NAME);
        }
    }
}

}

OpenMetricsTextOutputFormat::OpenMetricsTextOutputFormat(
    WriteBuffer & out_,
    SharedHeader header_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_)
    , string_serialization(DataTypeString().getDefaultSerialization())
    , format_settings(format_settings_)
{
    const Block & header = getPort(PortKind::Main).getHeader();

    getColumnPos(header, "name", isStringOrFixedString, pos.name);
    getColumnPos(header, "value", isNumber, pos.value);

    getColumnPos(header, "help", isStringOrFixedString, pos.help);
    getColumnPos(header, "type", isStringOrFixedString, pos.type);
    getColumnPos(header, "unit", isStringOrFixedString, pos.unit);
    getColumnPos(header, "timestamp", isInt64OrNullableInt64, pos.timestamp);
    getColumnPos(header, "labels", isDataTypeMapString, pos.labels);

    /// `getColumnPos` strips `Nullable` from optional columns before predicate validation, but
    /// `write` later casts `labels` straight to `ColumnMap`. Accepting `Nullable(Map(...))`
    /// here would silently drop labels for every row, so reject it explicitly.
    if (pos.labels.has_value() && header.getByName("labels").type->isNullable())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Illegal type '{}' of column 'labels' for output format '{}': Nullable(Map(...)) is not supported",
            header.getByName("labels").type->getName(), FORMAT_NAME);
}

/// Sort histogram/summary rows: regular buckets first (by `le`/`quantile`),
/// then `_sum`, then `_count`. Also synthesize `+Inf` bucket from `_count`
/// (or vice-versa) so the family always exposes both, matching Prometheus
/// exposition rules also referenced by OpenMetrics.
void OpenMetricsTextOutputFormat::fixupBucketLabels(CurrentMetric & metric)
{
    String bucket_label = metric.type == "histogram" ? "le" : "quantile";

    if (metric.type == "histogram")
    {
        struct SeriesState
        {
            bool has_count_marker = false;
            bool has_inf_bucket = false;
            std::optional<CurrentMetric::RowValue> count_marker_row;
            std::optional<CurrentMetric::RowValue> inf_bucket_row;
        };

        /// Synthesis is per (series, timestamp): a histogram series sampled at several timestamps
        /// must get its `+Inf`/`_count` counterpart at each timestamp independently, so the
        /// timestamp is part of the key. A series-only key would cross-match different timestamps
        /// (e.g. a `_count` at t1 and a `+Inf` at t2 would wrongly suppress synthesis for both).
        std::map<std::pair<std::map<String, String>, String>, SeriesState> series_states;
        for (const auto & val : metric.values)
        {
            auto & state = series_states[std::pair{histogramSeriesLabels(val.labels), val.timestamp}];

            if (isEmptyMarker(val.labels, "count"))
            {
                state.has_count_marker = true;
                state.count_marker_row = val;
            }
            if (auto it = val.labels.find("le"); it != val.labels.end() && it->second == "+Inf")
            {
                state.has_inf_bucket = true;
                state.inf_bucket_row = val;
            }
        }

        for (const auto & [key, state] : series_states)
        {
            const auto & series = key.first;
            if (state.has_count_marker && !state.has_inf_bucket && state.count_marker_row)
            {
                auto synthetic = *state.count_marker_row;
                synthetic.labels = series;
                synthetic.labels["le"] = "+Inf";
                metric.values.emplace_back(std::move(synthetic));
            }
            else if (!state.has_count_marker && state.has_inf_bucket && state.inf_bucket_row)
            {
                auto synthetic = *state.inf_bucket_row;
                synthetic.labels = series;
                synthetic.labels["count"] = "";
                metric.values.emplace_back(std::move(synthetic));
            }
        }
    }

    ::sort(metric.values.begin(), metric.values.end(),
        [&bucket_label](const auto & lhs, const auto & rhs)
        {
            bool lhs_sum = isEmptyMarker(lhs.labels, "sum");
            bool lhs_count = isEmptyMarker(lhs.labels, "count");
            bool lhs_meta = lhs_sum || lhs_count;

            bool rhs_sum = isEmptyMarker(rhs.labels, "sum");
            bool rhs_count = isEmptyMarker(rhs.labels, "count");
            bool rhs_meta = rhs_sum || rhs_count;

            if (lhs_sum && rhs_count)
                return true;
            if (lhs_count && rhs_sum)
                return false;
            if (rhs_meta && !lhs_meta)
                return true;
            if (lhs_meta && !rhs_meta)
                return false;

            auto lit = lhs.labels.find(bucket_label);
            auto rit = rhs.labels.find(bucket_label);
            if (lit != lhs.labels.end() && rit != rhs.labels.end())
            {
                const Float64 lval = tryParseFloat(lit->second);
                const Float64 rval = tryParseFloat(rit->second);
                if (lval != rval)
                    return lval < rval;
            }
            /// Same bucket bound and/or different series labels: fall back to a total order so
            /// multi-series histogram families are emitted deterministically across platforms.
            if (lhs.labels != rhs.labels)
                return lhs.labels < rhs.labels;
            /// Identical labels (same series + bucket) differing only by timestamp/value: e.g. one
            /// series sampled at several timestamps, or an original `+Inf`/`_count` row next to its
            /// synthesized counterpart. Without an explicit tiebreaker their relative order is the
            /// (chunk-dependent) input order, which diverges under parallel formatting. Order by
            /// timestamp then value for a deterministic, ascending-by-time output.
            const Float64 lts = tryParseFloat(lhs.timestamp);
            const Float64 rts = tryParseFloat(rhs.timestamp);
            if (lts != rts)
                return lts < rts;
            return lhs.value < rhs.value;
        });
}

void OpenMetricsTextOutputFormat::flushCurrentMetric()
{
    if (current_metric.name.empty() || current_metric.values.empty())
    {
        current_metric = {};
        return;
    }

    if (current_metric.type == "histogram" || current_metric.type == "summary")
    {
        for (const auto & val : current_metric.values)
        {
            /// Every row in a histogram/summary family must carry exactly one sample kind: a
            /// bucket/quantile sample, or a `_sum` / `_count` marker. Zero kinds (an untyped sample
            /// such as `# TYPE h histogram` + `h 3`) and more than one (combined kinds) are both
            /// invalid exposition.
            const size_t kinds = sampleKindCount(current_metric.type, val.labels);
            if (kinds == 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Row for output format '{}' in a '{}' family must carry exactly one sample kind "
                    "(a bucket/quantile sample or a '_sum' / '_count' marker), but has none",
                    FORMAT_NAME, current_metric.type);
            if (kinds > 1)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Row for output format '{}' cannot combine multiple histogram/summary sample kinds in labels",
                    FORMAT_NAME);

            /// Reject a non-empty `count` / `sum` label: those names are reserved markers in a
            /// histogram/summary family, so a real value collides with the synthesized `_count` /
            /// `_sum` counterpart (the writer would otherwise overwrite the real label and emit the
            /// marker for the wrong series).
            String offending_key;
            if (hasReservedMarkerLabelWithValue(val.labels, offending_key))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Label '{}' is a reserved histogram/summary marker for output format '{}' and must have an empty value",
                    offending_key, FORMAT_NAME);

            /// Validate the bucket/quantile boundary before `fixupBucketLabels` sorts numerically;
            /// the sort's lenient `tryParseFloat` would otherwise let `le='NaN'` / `quantile='bogus'`
            /// through. The rule is shared with the reader in `checkBoundaryLabel`; raise the writer's
            /// own BAD_ARGUMENTS here.
            if (auto reason = checkBoundaryLabel(current_metric.type, val.labels))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} for output format '{}'", *reason, FORMAT_NAME);
        }
    }

    auto write_attribute = [this](const char * marker, const String & value)
    {
        if (value.empty())
            return;
        writeCString(marker, out);
        writeString(current_metric.name, out);
        writeChar(' ', out);
        writeString(value, out);
        writeChar('\n', out);
    };

    write_attribute("# HELP ", current_metric.help);
    write_attribute("# TYPE ", current_metric.type);
    write_attribute("# UNIT ", current_metric.unit);

    bool use_buckets = current_metric.type == "histogram" || current_metric.type == "summary";
    if (use_buckets)
        fixupBucketLabels(current_metric);

    for (auto & val : current_metric.values)
    {
        for (const auto & [label_name, label_value] : val.labels)
            validateLabelValue(label_value);

        writeString(current_metric.name, out);

        /// Suffix-marker rewrite. The documented table contract is:
        ///   * `{'sum': ''}` / `{'count': ''}` are marker rows: append `_sum`/`_count` to the
        ///     family name and drop the marker label. A non-empty `sum`/`count` value is rejected
        ///     up front in `flushCurrentMetric` (reserved marker collision), so by here the guard
        ///     below only ever fires for the empty-marker case.
        ///   * `{'le': '<bound>'}` is only a histogram bucket. Summaries use `{quantile=...}`
        ///     and a real label named `le` on a summary is just a normal label, so the
        ///     `le` -> `_bucket` rewrite is gated on the histogram type.
        auto rewrite_empty_marker = [&val, this](const String & key, const String & suffix)
        {
            auto it = val.labels.find(key);
            if (it == val.labels.end() || !it->second.empty())
                return;
            writeChar('_', out);
            writeString(suffix, out);
            val.labels.erase(it);
        };

        if (use_buckets)
        {
            rewrite_empty_marker("sum", "sum");
            rewrite_empty_marker("count", "count");
            if (current_metric.type == "histogram")
            {
                if (auto it = val.labels.find("le"); it != val.labels.end())
                {
                    writeChar('_', out);
                    writeString("bucket", out);
                }
            }
        }

        if (!val.labels.empty())
        {
            writeChar('{', out);
            bool is_first = true;
            for (const auto & [name, value] : val.labels)
            {
                if (!is_first)
                    writeChar(',', out);
                is_first = false;
                writeString(name, out);
                writeChar('=', out);
                writeQuotedLabelValue(value, out);
            }
            writeChar('}', out);
        }

        writeChar(' ', out);

        if (val.value == "nan")
            writeString("NaN", out);
        else if (val.value == "inf")
            writeString("+Inf", out);
        else if (val.value == "-inf")
            writeString("-Inf", out);
        else
            writeString(val.value, out);

        if (!val.timestamp.empty())
        {
            writeChar(' ', out);
            writeString(val.timestamp, out);
        }

        writeChar('\n', out);
    }
    writeChar('\n', out);

    current_metric = {};
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

    String name = getString(columns, row_num, pos.name);
    validateOpenMetricsMetricName(name);
    if (current_metric.name != name)
    {
        flushCurrentMetric();
        current_metric = CurrentMetric(name);
    }

    if (pos.help.has_value() && !columns[*pos.help]->isNullAt(row_num) && current_metric.help.empty())
    {
        current_metric.help = getString(columns, row_num, *pos.help);
        std::replace(current_metric.help.begin(), current_metric.help.end(), '\n', ' ');
    }

    if (pos.type.has_value() && !columns[*pos.type]->isNullAt(row_num) && current_metric.type.empty())
    {
        current_metric.type = getString(columns, row_num, *pos.type);
        validateOpenMetricsType(current_metric.type);
    }

    std::optional<std::map<String, String>> labels;
    if (pos.labels.has_value())
    {
        if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(columns[*pos.labels].get()))
        {
            labels.emplace();
            columnMapToContainer(col_map, row_num, *labels);
        }
    }

    if (pos.unit.has_value() && !columns[*pos.unit]->isNullAt(row_num) && current_metric.unit.empty())
    {
        current_metric.unit = getString(columns, row_num, *pos.unit);
        std::replace(current_metric.unit.begin(), current_metric.unit.end(), '\n', ' ');
    }

    auto & row = current_metric.values.emplace_back();
    row.value = getString(columns, row_num, pos.value);

    /// OpenMetrics timestamp rule: emit whenever the column is present and non-NULL (including zero).
    /// Convert the ClickHouse millisecond representation to the OpenMetrics seconds string here.
    if (pos.timestamp.has_value() && !columns[*pos.timestamp]->isNullAt(row_num))
    {
        const IColumn & ts_col = *columns[*pos.timestamp];
        const Int64 ms = ts_col.isNullable()
            ? assert_cast<const ColumnNullable &>(ts_col).getNestedColumn().getInt(row_num)
            : ts_col.getInt(row_num);
        row.timestamp = millisToSecondsString(ms);
    }

    if (labels)
        row.labels = std::move(*labels);

    row_write_in_progress = false;
}

void OpenMetricsTextOutputFormat::finalizeImpl()
{
    if (!row_write_in_progress)
    {
        flushCurrentMetric();
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

    /// Intentionally drops the `version=1.0.0` parameter advertised by earlier iterations of this
    /// PR. This writer does not enforce strict OpenMetrics 1.0 family-metadata requirements
    /// (`# HELP`/`# TYPE`/`# UNIT` are emitted only when the columns are non-empty, and the `type`
    /// value is passed through verbatim, including the Prometheus-style `untyped`), so claiming
    /// version compliance would be misleading to strict consumers.
    factory.setContentType(FORMAT_NAME, "application/openmetrics-text; charset=utf-8");
    /// Each stream ends with `# EOF`; appending another exposition would make the file unreadable.
    factory.markFormatHasNoAppendSupport(FORMAT_NAME);
}

}
