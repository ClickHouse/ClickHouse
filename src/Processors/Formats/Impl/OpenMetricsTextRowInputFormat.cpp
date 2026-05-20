#include <Processors/Formats/Impl/OpenMetricsTextRowInputFormat.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/readFloatText.h>
#include <IO/readIntText.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
}

namespace
{
constexpr auto FORMAT_NAME = "OpenMetrics";

void skipAsciiSpaces(std::string_view s, size_t & pos)
{
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t'))
        ++pos;
}

bool isPrometheusIdentifierChar(char c, bool first)
{
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == ':')
        return true;
    return !first && c >= '0' && c <= '9';
}

/// Prometheus / OpenMetrics metric and label names: [a-zA-Z_:][a-zA-Z0-9_:]*
bool isValidPrometheusIdentifier(std::string_view id)
{
    if (id.empty() || !isPrometheusIdentifierChar(id[0], true))
        return false;
    for (size_t i = 1; i < id.size(); ++i)
        if (!isPrometheusIdentifierChar(id[i], false))
            return false;
    return true;
}

bool tryConsume(std::string_view s, size_t & pos, char c)
{
    skipAsciiSpaces(s, pos);
    if (pos < s.size() && s[pos] == c)
    {
        ++pos;
        return true;
    }
    return false;
}

/// Read a Prometheus/OpenMetrics double-quoted string; `pos` at opening `"`.
/// Escape sequences: `\\`, `\"`, `\n` (https://prometheus.io/docs/instrumenting/exposition_formats/).
bool readQuotedLabelValue(std::string_view s, size_t & pos, String & out)
{
    if (pos >= s.size() || s[pos] != '"')
        return false;
    ++pos;
    out.clear();
    while (pos < s.size())
    {
        if (s[pos] == '"')
        {
            ++pos;
            return true;
        }
        if (s[pos] == '\\')
        {
            ++pos;
            if (pos >= s.size())
                return false;
            switch (s[pos])
            {
                case '\\':
                    out.push_back('\\');
                    break;
                case '"':
                    out.push_back('"');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                default:
                    return false;
            }
            ++pos;
            continue;
        }
        out.push_back(s[pos]);
        ++pos;
    }
    return false;
}

/// `pos` at opening `{` of a label set.
bool parseLabelSet(std::string_view s, size_t & pos, std::map<String, String> & labels)
{
    if (pos >= s.size() || s[pos] != '{')
        return false;
    ++pos;
    while (true)
    {
        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == '}')
        {
            ++pos;
            return true;
        }
        size_t key_start = pos;
        while (pos < s.size() && s[pos] != '=' && s[pos] != '}')
            ++pos;
        if (pos >= s.size())
            return false;
        String key{s.substr(key_start, pos - key_start)};
        if (!isValidPrometheusIdentifier(key))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid label name '{}' in OpenMetrics label set", key);
        if (!tryConsume(s, pos, '='))
            return false;
        String value;
        if (!readQuotedLabelValue(s, pos, value))
            return false;
        auto [it, inserted] = labels.emplace(std::move(key), std::move(value));
        if (!inserted)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate label name '{}' in OpenMetrics label set", it->first);
        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == ',')
        {
            ++pos;
            continue;
        }
        if (pos < s.size() && s[pos] == '}')
        {
            ++pos;
            return true;
        }
        return false;
    }
}

/// `pos` at first char of metric stem (before `{`, ASCII space, or tab — whitespace before value).
bool parseMetricDescriptor(std::string_view s, size_t & pos, String & stem, std::map<String, String> & labels)
{
    size_t stem_start = pos;
    while (pos < s.size() && s[pos] != '{' && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    stem = String{s.substr(stem_start, pos - stem_start)};
    if (!stem.empty() && !isValidPrometheusIdentifier(stem))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid metric name '{}' in OpenMetrics line", stem);

    if (pos < s.size())
    {
        if (s[pos] == '{')
        {
            if (!parseLabelSet(s, pos, labels))
                return false;
        }
        else if (s[pos] == ' ' || s[pos] == '\t')
        {
            size_t peek = pos;
            skipAsciiSpaces(s, peek);
            if (peek < s.size() && s[peek] == '{')
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Whitespace between metric name and label set is not allowed in OpenMetrics line");
        }
    }
    return true;
}

bool parseValueToken(std::string_view s, size_t & pos, String & value_out)
{
    skipAsciiSpaces(s, pos);
    size_t start = pos;
    while (pos < s.size() && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    value_out = String{s.substr(start, pos - start)};
    return !value_out.empty();
}

/// Optional sample or exemplar timestamp (`realnumber` in OpenMetrics text grammar).
bool parseOptionalTimestampToken(std::string_view s, size_t & pos, String & out, const String & line)
{
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] == '#')
        return false;

    const size_t start = pos;
    while (pos < s.size() && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    out = String{s.substr(start, pos - start)};

    Float64 v = 0;
    ReadBufferFromString buf(out);
    if (!tryReadFloatText(v, buf) || !buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid timestamp token '{}' in OpenMetrics line: {}", out, line);
    return true;
}

Int64 timestampTokenToInt64(const String & token, const String & line)
{
    Float64 v = 0;
    ReadBufferFromString buf(token);
    if (!tryReadFloatText(v, buf) || !buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid timestamp token '{}' in OpenMetrics line: {}", token, line);
    if (!std::isfinite(v) || v < static_cast<Float64>(std::numeric_limits<Int64>::min())
        || v > static_cast<Float64>(std::numeric_limits<Int64>::max()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Timestamp value out of Int64 range in OpenMetrics line: {}", line);
    return static_cast<Int64>(v);
}

/// Optional exemplar after sample value / timestamp: `# {labels} <value>` (OpenMetrics text).
void consumeOptionalExemplarSuffix(std::string_view s, size_t & pos, const String & line)
{
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] != '#')
        return;

    ++pos;
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] != '{')
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid exemplar in OpenMetrics line: {}", line);

    std::map<String, String> exemplar_labels;
    if (!parseLabelSet(s, pos, exemplar_labels))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid exemplar label set in OpenMetrics line: {}", line);

    String exemplar_value_token;
    if (!parseValueToken(s, pos, exemplar_value_token))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse exemplar value in OpenMetrics line: {}", line);

    Float64 exemplar_value = 0;
    ReadBufferFromString buf(exemplar_value_token);
    if (!tryReadFloatText(exemplar_value, buf) || !buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse exemplar value '{}' in OpenMetrics line: {}", exemplar_value_token, line);

    String exemplar_ts_token;
    parseOptionalTimestampToken(s, pos, exemplar_ts_token, line);
}

void checkMetricLineFullyConsumed(std::string_view s, size_t & pos, const String & line)
{
    skipAsciiSpaces(s, pos);
    if (pos < s.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in OpenMetrics line: {}", line);
}

void insertFloatFromPrometheusText(IColumn & column, const String & token)
{
    Float64 v = 0;
    if (token == "NaN")
        v = std::numeric_limits<double>::quiet_NaN();
    else if (token == "+Inf" || token == "Inf")
        v = std::numeric_limits<double>::infinity();
    else if (token == "-Inf")
        v = -std::numeric_limits<double>::infinity();
    else
    {
        ReadBufferFromString buf(token);
        if (!tryReadFloatText(v, buf) || !buf.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse float value '{}' in OpenMetrics format", token);
    }
    assert_cast<ColumnFloat64 &>(column).insert(v);
}

bool isDataTypeMapStringString(const DataTypePtr & type)
{
    if (!isMap(type))
        return false;
    const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
    const auto & kt = type_map->getKeyType();
    const auto & vt = type_map->getValueType();
    /// Insertion uses `ColumnMap` built from `Map(String, String)` materialization — require plain `String`, not
    /// `FixedString` / nullable variants that would not match `assert_cast` targets in release builds.
    return !kt->isNullable() && !vt->isNullable() && WhichDataType(kt).isString() && WhichDataType(vt).isString();
}

bool isOpenMetricsStringColumnType(const DataTypePtr & type)
{
    return !type->isNullable() && WhichDataType(type).isString();
}

bool isOpenMetricsValueType(const DataTypePtr & type)
{
    /// `insertFloatFromPrometheusText` always writes `ColumnFloat64`; reject `Nullable(Float64)` etc.
    return !type->isNullable() && WhichDataType(type).isFloat64();
}

bool isOpenMetricsTimestampType(const DataTypePtr & type)
{
    if (type->isNullable())
    {
        const auto * nullable = assert_cast<const DataTypeNullable *>(type.get());
        return WhichDataType(nullable->getNestedType()).isInt64();
    }
    return WhichDataType(type).isInt64();
}

void checkColumnType(const Block & header, const String & col_name, bool (*pred)(const DataTypePtr &))
{
    const auto & col = header.getByName(col_name);
    if (!pred(col.type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Illegal type '{}' of column '{}' for input format '{}'",
            col.type->getName(),
            col_name,
            FORMAT_NAME);
}

void insertMapLabels(IColumn & column, const std::map<String, String> & labels)
{
    Field map_field = Map();
    Map & m = map_field.safeGet<Map>();
    for (const auto & [k, v] : labels)
        m.push_back(Tuple{k, v});
    column.insert(map_field);
}

bool isBlankAsciiLine(const String & line)
{
    for (char c : line)
        if (c != ' ' && c != '\t')
            return false;
    return true;
}

/// `# EOF` optionally followed only by ASCII space/tab on the same line (no other tokens).
bool isOpenMetricsEofLine(const String & line)
{
    static constexpr std::string_view prefix = "# EOF";
    if (!line.starts_with(prefix))
        return false;
    for (size_t i = prefix.size(); i < line.size(); ++i)
    {
        if (line[i] != ' ' && line[i] != '\t')
            return false;
    }
    return true;
}

/// After `# EOF`, the exposition stream is logically finished; reject trailing payload.
void throwIfNonBlankAfterEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        String tail_line;
        readStringUntilNewlineInto(tail_line, buf);
        if (!buf.eof())
            buf.ignore();
        if (!tail_line.empty() && tail_line.back() == '\r')
            tail_line.pop_back();
        if (!tail_line.empty() && !isBlankAsciiLine(tail_line))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after # EOF in OpenMetrics input");
    }
}

}

OpenMetricsTextRowInputFormat::ColumnLoc OpenMetricsTextRowInputFormat::buildColumnLoc(const Block & header)
{
    ColumnLoc loc;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & col_name = header.getByPosition(i).name;
        if (col_name == "name")
            loc.name = i;
        else if (col_name == "value")
            loc.value = i;
        else if (col_name == "help")
            loc.help = i;
        else if (col_name == "type")
            loc.type = i;
        else if (col_name == "labels")
            loc.labels = i;
        else if (col_name == "timestamp")
            loc.timestamp = i;
        else if (col_name == "unit")
            loc.unit = i;
    }
    if (!loc.name || !loc.value)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Format '{}' requires 'name' and 'value' columns", FORMAT_NAME);

    checkColumnType(header, "name", isOpenMetricsStringColumnType);
    checkColumnType(header, "value", isOpenMetricsValueType);
    if (loc.help)
        checkColumnType(header, "help", isOpenMetricsStringColumnType);
    if (loc.type)
        checkColumnType(header, "type", isOpenMetricsStringColumnType);
    if (loc.labels)
        checkColumnType(header, "labels", isDataTypeMapStringString);
    if (loc.timestamp)
        checkColumnType(header, "timestamp", isOpenMetricsTimestampType);
    if (loc.unit)
        checkColumnType(header, "unit", isOpenMetricsStringColumnType);

    return loc;
}

OpenMetricsTextRowInputFormat::OpenMetricsTextRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
    , format_settings(format_settings_)
{
}

void OpenMetricsTextRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    family_meta.clear();
    saw_eof = false;
    column_loc_initialized = false;
}

void OpenMetricsTextRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

bool OpenMetricsTextRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!column_loc_initialized)
    {
        column_loc = buildColumnLoc(getPort().getHeader());
        column_loc_initialized = true;
    }
    const ColumnLoc & loc = column_loc;

    /// Like JSONEachRowRowInputFormat::checkEndOfData: if we return false with no row, leave read_columns
    /// empty so IRowInputFormat does not treat all-zero read_columns as "missing values" (Code 7).
    ext.read_columns.clear();

    while (!in->eof() && !saw_eof)
    {
        String line;
        readStringUntilNewlineInto(line, *in);
        if (!in->eof())
            in->ignore(); /// Skip '\n'

        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        if (line.empty())
            continue;

        if (line.starts_with("#"))
        {
            if (line.starts_with("# EOF"))
            {
                if (!isOpenMetricsEofLine(line))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid # EOF line in OpenMetrics input");
                throwIfNonBlankAfterEOF(*in);
                saw_eof = true;
                continue;
            }

            /// `# HELP name text`
            if (line.starts_with("# HELP "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                String help{sv.substr(p)};
                family_meta[name].help = String(help);
                continue;
            }
            if (line.starts_with("# TYPE "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                /// Type is a single token (counter, gauge, histogram, summary, unknown); ignore trailing spaces.
                size_t type_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String typ{sv.substr(type_start, p - type_start)};
                family_meta[name].type = String(typ);
                continue;
            }
            if (line.starts_with("# UNIT "))
            {
                std::string_view sv{line};
                size_t p = 7;
                skipAsciiSpaces(sv, p);
                size_t name_start = p;
                while (p < sv.size() && sv[p] != ' ' && sv[p] != '\t')
                    ++p;
                String name{sv.substr(name_start, p - name_start)};
                skipAsciiSpaces(sv, p);
                String unit{sv.substr(p)};
                family_meta[name].unit = String(unit);
                continue;
            }
            /// Other metadata / comments: ignore
            continue;
        }

        /// Metric line
        std::string_view sv{line};
        size_t pos = 0;
        String stem;
        std::map<String, String> labels;
        if (!parseMetricDescriptor(sv, pos, stem, labels))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse labels in OpenMetrics line: {}", line);

        if (stem.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Empty metric name in OpenMetrics line: {}", line);

        if (pos >= sv.size() || (sv[pos] != ' ' && sv[pos] != '\t'))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Missing whitespace between metric descriptor and value in OpenMetrics line: {}",
                line);

        String value_token;
        if (!parseValueToken(sv, pos, value_token))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse value in OpenMetrics line: {}", line);

        String ts_token;
        bool has_ts = parseOptionalTimestampToken(sv, pos, ts_token, line);
        consumeOptionalExemplarSuffix(sv, pos, line);
        checkMetricLineFullyConsumed(sv, pos, line);

        /// Resolve logical metric name (histogram/summary suffixes).
        String logical_name = stem;

        auto applyStemSuffix = [&]()
        {
            if (stem.ends_with("_bucket") && stem.size() > 7)
            {
                String base{stem.substr(0, stem.size() - 7)};
                auto it = family_meta.find(base);
                if (it != family_meta.end() && (it->second.type == "histogram" || it->second.type == "summary"))
                    logical_name = base;
            }
            else if (stem.ends_with("_sum") && stem.size() > 4)
            {
                String base{stem.substr(0, stem.size() - 4)};
                auto it = family_meta.find(base);
                if (it != family_meta.end() && (it->second.type == "histogram" || it->second.type == "summary"))
                {
                    logical_name = base;
                    labels["sum"] = "";
                }
            }
            else if (stem.ends_with("_count") && stem.size() > 6)
            {
                String base{stem.substr(0, stem.size() - 6)};
                auto it = family_meta.find(base);
                if (it != family_meta.end() && (it->second.type == "histogram" || it->second.type == "summary"))
                {
                    logical_name = base;
                    labels["count"] = "";
                }
            }
        };
        applyStemSuffix();

        /// Do not use operator[] here: it would insert an empty entry for every unseen metric name
        /// (high-cardinality streams). Metadata comes only from # HELP / # TYPE / # UNIT lines.
        static const FamilyMeta empty_family_meta;
        auto meta_it = family_meta.find(logical_name);
        const FamilyMeta & fm = (meta_it != family_meta.end()) ? meta_it->second : empty_family_meta;

        ext.read_columns.assign(columns.size(), 0);

        if (loc.name)
        {
            assert_cast<ColumnString &>(*columns[*loc.name]).insertData(logical_name.data(), logical_name.size());
            ext.read_columns[*loc.name] = 1;
        }
        if (loc.value)
        {
            insertFloatFromPrometheusText(*columns[*loc.value], value_token);
            ext.read_columns[*loc.value] = 1;
        }
        if (loc.help)
        {
            assert_cast<ColumnString &>(*columns[*loc.help]).insertData(fm.help.data(), fm.help.size());
            ext.read_columns[*loc.help] = 1;
        }
        if (loc.type)
        {
            assert_cast<ColumnString &>(*columns[*loc.type]).insertData(fm.type.data(), fm.type.size());
            ext.read_columns[*loc.type] = 1;
        }
        if (loc.labels)
        {
            insertMapLabels(*columns[*loc.labels], labels);
            ext.read_columns[*loc.labels] = 1;
        }
        if (loc.timestamp)
        {
            auto & col = *columns[*loc.timestamp];
            if (col.isNullable())
            {
                auto & nullable_col = assert_cast<ColumnNullable &>(col);
                if (has_ts)
                {
                    const Int64 t = timestampTokenToInt64(ts_token, line);
                    nullable_col.getNestedColumn().insert(t);
                    nullable_col.getNullMapColumn().insertValue(0);
                }
                else
                    nullable_col.insertDefault();
            }
            else
            {
                if (!has_ts)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Timestamp column is not Nullable but line has no timestamp: {}", line);
                assert_cast<ColumnInt64 &>(col).insert(timestampTokenToInt64(ts_token, line));
            }
            ext.read_columns[*loc.timestamp] = 1;
        }
        if (loc.unit)
        {
            assert_cast<ColumnString &>(*columns[*loc.unit]).insertData(fm.unit.data(), fm.unit.size());
            ext.read_columns[*loc.unit] = 1;
        }

        return true;
    }

    ext.read_columns.clear();
    return false;
}

NamesAndTypesList OpenMetricsTextSchemaReader::readSchema()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeFloat64>()},
        {"help", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"labels", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"unit", std::make_shared<DataTypeString>()},
    };
}

void registerInputFormatOpenMetrics(FormatFactory & factory)
{
    factory.registerInputFormat(
        FORMAT_NAME,
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        { return std::make_shared<OpenMetricsTextRowInputFormat>(std::make_shared<const Block>(sample), buf, std::move(params), settings); });

    factory.registerExternalSchemaReader(
        FORMAT_NAME,
        [](const FormatSettings &)
        { return std::make_shared<OpenMetricsTextSchemaReader>(); });

    factory.markFormatSupportsSubsetOfColumns(FORMAT_NAME);
}

}
