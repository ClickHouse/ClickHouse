#include <Processors/Formats/Impl/OpenMetricsTextOutputFormat.h>

#include <algorithm>
#include <optional>
#include <type_traits>

#include <base/sort.h>

#include <Columns/ColumnMap.h>
#include <Columns/IColumn.h>

#include <Common/assert_cast.h>

#include <Core/Field.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
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

constexpr auto FORMAT_NAME = "OpenMetrics";

bool isDataTypeMapString(const DataTypePtr & type)
{
    if (!isMap(type))
        return false;
    const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
    return isStringOrFixedString(type_map->getKeyType()) && isStringOrFixedString(type_map->getValueType());
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
            result.emplace(entry_key, entry_value);
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
    getColumnPos(header, "timestamp", isNumber, pos.timestamp);
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

    ::sort(metric.values.begin(), metric.values.end(),
        [&bucket_label](const auto & lhs, const auto & rhs)
        {
            bool lhs_sum = lhs.labels.contains("sum");
            bool lhs_count = lhs.labels.contains("count");
            bool lhs_meta = lhs_sum || lhs_count;

            bool rhs_sum = rhs.labels.contains("sum");
            bool rhs_count = rhs.labels.contains("count");
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
                return tryParseFloat(lit->second) < tryParseFloat(rit->second);
            return false;
        });

    if (metric.type == "histogram")
    {
        std::optional<CurrentMetric::RowValue> inf_bucket;
        std::optional<CurrentMetric::RowValue> count_bucket;
        for (const auto & val : metric.values)
        {
            if (auto it = val.labels.find("count"); it != val.labels.end())
            {
                inf_bucket = val;
                inf_bucket->labels = {{"le", "+Inf"}};
            }
            if (auto it = val.labels.find("le"); it != val.labels.end() && it->second == "+Inf")
            {
                count_bucket = val;
                count_bucket->labels = {{"count", ""}};
            }
        }
        if (inf_bucket.has_value() && !count_bucket.has_value())
            metric.values.emplace_back(*inf_bucket);

        if (!inf_bucket.has_value() && count_bucket.has_value())
            metric.values.emplace_back(*count_bucket);
    }
}

void OpenMetricsTextOutputFormat::flushCurrentMetric()
{
    if (current_metric.name.empty() || current_metric.values.empty())
    {
        current_metric = {};
        return;
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
        writeString(current_metric.name, out);

        auto label_to_suffix = [&val, this](const String & key, const String & suffix, bool erase)
        {
            if (auto it = val.labels.find(key); it != val.labels.end())
            {
                writeChar('_', out);
                writeString(suffix, out);
                if (erase)
                    val.labels.erase(it);
            }
        };

        if (use_buckets)
        {
            label_to_suffix("sum", "sum", true);
            label_to_suffix("count", "count", true);
            label_to_suffix("le", "bucket", false);
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
                writeDoubleQuotedString(value, out);
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
    String name = getString(columns, row_num, pos.name);
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
        current_metric.type = getString(columns, row_num, *pos.type);

    if (pos.unit.has_value() && !columns[*pos.unit]->isNullAt(row_num) && current_metric.unit.empty())
    {
        current_metric.unit = getString(columns, row_num, *pos.unit);
        std::replace(current_metric.unit.begin(), current_metric.unit.end(), '\n', ' ');
    }

    auto & row = current_metric.values.emplace_back();
    row.value = getString(columns, row_num, pos.value);

    /// OpenMetrics timestamp rule: emit whenever the column is present and non-NULL (including zero).
    if (pos.timestamp.has_value() && !columns[*pos.timestamp]->isNullAt(row_num))
        row.timestamp = getString(columns, row_num, *pos.timestamp);

    if (pos.labels.has_value())
    {
        if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(columns[*pos.labels].get()))
            columnMapToContainer(col_map, row_num, row.labels);
    }
}

void OpenMetricsTextOutputFormat::finalizeImpl()
{
    flushCurrentMetric();
    writeCString("# EOF\n", out);
}

void registerOutputFormatOpenMetrics(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<OpenMetricsTextOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.setContentType(FORMAT_NAME, "application/openmetrics-text; version=1.0.0; charset=utf-8");
}

}
