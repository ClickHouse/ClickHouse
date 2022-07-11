#include <Processors/Formats/Impl/PrometheusTextOutputFormat.h>

#include <optional>
#include <type_traits>

#include <base/defines.h>
#include <base/types.h>
#include <base/sort.h>

#include <Columns/ColumnMap.h>
#include <Columns/IColumn.h>

#include <Common/assert_cast.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>

#include <IO/ReadBufferFromString.h>
#include <IO/readFloatText.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

constexpr auto FORMAT_NAME = "Prometheus";

static bool isDataTypeMapString(const DataTypePtr & type)
{
    if (!isMap(type))
        return false;
    const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
    return isStringOrFixedString(type_map->getKeyType()) && isStringOrFixedString(type_map->getValueType());
}

template <typename ResType, typename Pred>
static void getColumnPos(const Block & header, const String & col_name, Pred pred, ResType & res)
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

static Float64 tryParseFloat(const String & s)
{
    Float64 t = 0;
    ReadBufferFromString buf(s);
    tryReadFloatText(t, buf);
    return t;
}

PrometheusTextOutputFormat::PrometheusTextOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_)
    , string_serialization(DataTypeString().getDefaultSerialization())
    , format_settings(format_settings_)
{
    const Block & header = getPort(PortKind::Main).getHeader();

    getColumnPos(header, "name", isStringOrFixedString<DataTypePtr>, pos.name);
    getColumnPos(header, "value", isNumber<DataTypePtr>, pos.value);

    getColumnPos(header, "help", isStringOrFixedString<DataTypePtr>, pos.help);
    getColumnPos(header, "type", isStringOrFixedString<DataTypePtr>, pos.type);
    getColumnPos(header, "timestamp", isNumber<DataTypePtr>, pos.timestamp);
    getColumnPos(header, "labels", isDataTypeMapString, pos.labels);
}

/*
 * https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries
 *
 * > A histogram must have a bucket with {le="+Inf"}. Its value must be identical to the value of x_count.
 * > The buckets of a histogram and the quantiles of a summary must appear in increasing numerical order of their label values (for the le or the quantile label, respectively).
*/
void PrometheusTextOutputFormat::fixupBucketLabels(CurrentMetric & metric)
{
    String bucket_label = metric.type == "histogram" ? "le" : "quantile";

    ::sort(metric.values.begin(), metric.values.end(),
        [&bucket_label](const auto & lhs, const auto & rhs)
        {
            bool lhs_labels_contain_sum = lhs.labels.contains("sum");
            bool lhs_labels_contain_count = lhs.labels.contains("count");
            bool lhs_labels_contain_sum_or_count = lhs_labels_contain_sum || lhs_labels_contain_count;

            bool rhs_labels_contain_sum = rhs.labels.contains("sum");
            bool rhs_labels_contain_count = rhs.labels.contains("count");
            bool rhs_labels_contain_sum_or_count = rhs_labels_contain_sum || rhs_labels_contain_count;

            /// rows with labels at the beginning and then `_sum` and `_count`
            if (lhs_labels_contain_sum && rhs_labels_contain_count)
                return true;
            else if (lhs_labels_contain_count && rhs_labels_contain_sum)
                return false;
            else if (rhs_labels_contain_sum_or_count && !lhs_labels_contain_sum_or_count)
                return true;
            else if (lhs_labels_contain_sum_or_count && !rhs_labels_contain_sum_or_count)
                return false;

            auto lit = lhs.labels.find(bucket_label);
            auto rit = rhs.labels.find(bucket_label);
            if (lit != lhs.labels.end() && rit != rhs.labels.end())
                return tryParseFloat(lit->second) < tryParseFloat(rit->second);
            return false;
        });

    if (metric.type == "histogram")
    {
        /// If we have only `_count` or metric with `le="+Inf" need to create both
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

void PrometheusTextOutputFormat::flushCurrentMetric()
{
    if (current_metric.name.empty() || current_metric.values.empty())
    {
        current_metric = {};
        return;
    }

    auto write_attribute = [this] (const char * name, const auto & value)
    {
        if (value.empty())
            return;

        writeCString(name, out);
        writeString(current_metric.name, out);
        writeChar(' ', out);
        writeString(value, out);
        writeChar('\n', out);
    };

    write_attribute("# HELP ", current_metric.help);
    write_attribute("# TYPE ", current_metric.type);

    bool use_buckets = current_metric.type == "histogram" || current_metric.type == "summary";
    if (use_buckets)
    {
        fixupBucketLabels(current_metric);
    }

    for (auto & val : current_metric.values)
    {
        /* https://prometheus.io/docs/instrumenting/exposition_formats/#comments-help-text-and-type-information
         ```
         metric_name [
           "{" label_name "=" `"` label_value `"` { "," label_name "=" `"` label_value `"` } [ "," ] "}"
         ] value [ timestamp ]
         ```
        */
        writeString(current_metric.name, out);

        auto lable_to_suffix = [&val, this](const auto & key, const auto & suffix, bool erase)
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
            lable_to_suffix("sum", "sum", true);
            lable_to_suffix("count", "count", true);
            lable_to_suffix("le", "bucket", false);
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

String PrometheusTextOutputFormat::getString(const Columns & columns, size_t row_num, size_t column_pos)
{
    WriteBufferFromOwnString tout;
    serializations[column_pos]->serializeText(*columns[column_pos], row_num, tout, format_settings);
    return tout.str();
}

String PrometheusTextOutputFormat::getString(const IColumn & column, size_t row_num, SerializationPtr serialization)
{
    WriteBufferFromOwnString tout;
    serialization->serializeText(column, row_num, tout, format_settings);
    return tout.str();
}

template <typename Container>
static void columnMapToContainer(const ColumnMap * col_map, size_t row_num, Container & result)
{
    Field field;
    col_map->get(row_num, field);
    const auto & map_field = field.get<Map>();
    for (size_t i = 0; i < map_field.size(); ++i)
    {
        const auto & map_entry = map_field[i].get<Tuple>();

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

void PrometheusTextOutputFormat::write(const Columns & columns, size_t row_num)
{
    String name = getString(columns, row_num, pos.name);
    if (current_metric.name != name)
    {
        flushCurrentMetric();
        current_metric = CurrentMetric(name);
    }

    if (pos.help.has_value() && !columns[*pos.help]->isNullAt(row_num) && current_metric.help.empty())
        current_metric.help = getString(columns, row_num, *pos.help);

    if (pos.type.has_value() && !columns[*pos.type]->isNullAt(row_num) && current_metric.type.empty())
        current_metric.type = getString(columns, row_num, *pos.type);

    auto & row = current_metric.values.emplace_back();

    row.value = getString(columns, row_num, pos.value);

    if (pos.timestamp.has_value() && !columns[*pos.timestamp]->isNullAt(row_num) && columns[*pos.timestamp]->get64(row_num) != 0)
        row.timestamp = getString(columns, row_num, *pos.timestamp);

    if (pos.labels.has_value())
    {
        if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(columns[*pos.labels].get()))
            columnMapToContainer(col_map, row_num, row.labels);
    }
}

void PrometheusTextOutputFormat::finalizeImpl()
{
    flushCurrentMetric();
}

void registerOutputFormatPrometheus(FormatFactory & factory)
{
    factory.registerOutputFormat(FORMAT_NAME, [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<PrometheusTextOutputFormat>(buf, sample, params, settings);
    });
}

}
