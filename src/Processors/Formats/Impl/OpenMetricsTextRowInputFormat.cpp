#include <Processors/Formats/Impl/OpenMetricsTextRowInputFormat.h>

#include <Processors/Formats/Impl/OpenMetricsText.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readFloatText.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <map>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

namespace
{
using OpenMetricsText::FORMAT_NAME;
using OpenMetricsText::throwIncorrect;
using OpenMetricsText::isValidName;
using OpenMetricsText::readQuotedLabelValue;
using OpenMetricsText::equalsIgnoreCaseAscii;
using OpenMetricsText::isStrictRealNumberToken;
using OpenMetricsText::secondsTokenToMillis;

void skipAsciiSpaces(std::string_view s, size_t & pos)
{
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t'))
        ++pos;
}

/// Returns the substring up to the next ASCII space/tab; advances `pos`.
std::string_view readToken(std::string_view s, size_t & pos)
{
    const size_t start = pos;
    while (pos < s.size() && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    return s.substr(start, pos - start);
}

/// `pos` at `{`. Validates label names (no `:`), rejects duplicates, throws on malformed input.
void parseLabelSet(std::string_view s, size_t & pos, std::map<String, String> & labels, const String & line)
{
    if (pos >= s.size() || s[pos] != '{')
        throwIncorrect("Cannot parse labels", line);
    ++pos;
    while (true)
    {
        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == '}') { ++pos; return; }

        const size_t key_start = pos;
        while (pos < s.size() && s[pos] != '=' && s[pos] != '}')
            ++pos;
        const std::string_view key = s.substr(key_start, pos - key_start);
        if (!isValidName(key, /*allow_colon=*/false))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid label name '{}' in OpenMetrics label set", key);
        if (pos >= s.size() || s[pos] != '=')
            throwIncorrect("Invalid label set", line);
        ++pos;

        String value;
        if (!readQuotedLabelValue(s, pos, value))
            throwIncorrect("Invalid label set", line);

        auto [it, inserted] = labels.emplace(String(key), std::move(value));
        if (!inserted)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate label name '{}' in OpenMetrics label set", it->first);

        skipAsciiSpaces(s, pos);
        if (pos < s.size() && s[pos] == ',') { ++pos; continue; }
        if (pos < s.size() && s[pos] == '}') { ++pos; return; }
        throwIncorrect("Invalid label set", line);
    }
}

/// Parses `<name>[{labels}]`. Rejects empty name, invalid identifiers, and whitespace before `{`.
void parseMetricDescriptor(std::string_view s, size_t & pos, String & stem, std::map<String, String> & labels, const String & line)
{
    const size_t name_start = pos;
    while (pos < s.size() && s[pos] != '{' && s[pos] != ' ' && s[pos] != '\t')
        ++pos;
    const std::string_view name = s.substr(name_start, pos - name_start);

    if (name.empty())
        throwIncorrect("Empty metric name", line);
    if (!isValidName(name, /*allow_colon=*/true))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid metric name '{}' in OpenMetrics line", name);
    stem = String{name};

    if (pos >= s.size())
        return;
    if (s[pos] == '{')
    {
        parseLabelSet(s, pos, labels, line);
        return;
    }
    /// Detect `metric  {labels}` — labels must immediately follow the metric name.
    size_t peek = pos;
    skipAsciiSpaces(s, peek);
    if (peek < s.size() && s[peek] == '{')
        throwIncorrect("Whitespace between metric name and label set is not allowed", line);
}

/// `realnumber` per OpenMetrics ABNF: optional sign + digits + optional fractional / exponent; rejects NaN/Inf.
Float64 parseRealNumber(std::string_view token, const String & line)
{
    if (token == "NaN" || token == "+Inf" || token == "Inf" || token == "-Inf")
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid timestamp token '{}' in OpenMetrics line: {}", token, line);

    if (!isStrictRealNumberToken(token))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid timestamp token '{}' in OpenMetrics line: {}", token, line);

    Float64 v = 0;
    ReadBufferFromString buf(token);
    if (!tryReadFloatTextPrecise(v, buf) || !buf.eof() || !std::isfinite(v))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid timestamp token '{}' in OpenMetrics line: {}", token, line);
    return v;
}

/// OpenMetrics `number`: a real number, or the special values NaN / ±Inf. Special values are ASCII
/// case-insensitive, and infinity may be spelled `inf` or `infinity` with an optional leading sign
/// (`nan` carries no sign). Requires full-token consumption for the real-number path.
Float64 parseSampleValue(std::string_view token)
{
    if (equalsIgnoreCaseAscii(token, "nan"))
        return std::numeric_limits<double>::quiet_NaN();

    std::string_view inf_token = token;
    bool inf_negative = false;
    if (!inf_token.empty() && (inf_token.front() == '+' || inf_token.front() == '-'))
    {
        inf_negative = inf_token.front() == '-';
        inf_token.remove_prefix(1);
    }
    if (equalsIgnoreCaseAscii(inf_token, "inf") || equalsIgnoreCaseAscii(inf_token, "infinity"))
        return inf_negative
            ? -std::numeric_limits<double>::infinity()
            : std::numeric_limits<double>::infinity();

    if (!isStrictRealNumberToken(token))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse float value '{}' in OpenMetrics format", token);

    Float64 v = 0;
    ReadBufferFromString buf(token);
    if (!tryReadFloatTextPrecise(v, buf) || !buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse float value '{}' in OpenMetrics format", token);
    return v;
}

/// Parses optional `# {labels} <value> [<timestamp>]` after sample value/timestamp.
/// Exemplar payload is validated but not ingested into the row schema.
void parseExemplarSuffix(std::string_view s, size_t & pos, const String & line)
{
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] != '#')
        return;
    ++pos;
    if (pos >= s.size() || (s[pos] != ' ' && s[pos] != '\t'))
        throwIncorrect("Invalid exemplar", line);
    skipAsciiSpaces(s, pos);
    if (pos >= s.size() || s[pos] != '{')
        throwIncorrect("Invalid exemplar", line);

    std::map<String, String> exemplar_labels;
    parseLabelSet(s, pos, exemplar_labels, line);

    if (pos >= s.size() || (s[pos] != ' ' && s[pos] != '\t'))
        throwIncorrect("Invalid exemplar", line);
    skipAsciiSpaces(s, pos);
    const std::string_view value_tok = readToken(s, pos);
    if (value_tok.empty())
        throwIncorrect("Cannot parse exemplar value", line);
    parseSampleValue(value_tok);

    skipAsciiSpaces(s, pos);
    if (pos < s.size())
        parseRealNumber(readToken(s, pos), line);
}

/// Splits `# <PREFIX> <name>[ <rest>]` after `prefix_len` chars (including the trailing space of the prefix).
/// The family name follows the same grammar as a sample's metric name.
void parseMetadataLine(const String & line, size_t prefix_len, String & name, String & rest)
{
    std::string_view sv{line};
    size_t p = prefix_len;
    skipAsciiSpaces(sv, p);
    name = String{readToken(sv, p)};
    if (!isValidName(name, /*allow_colon=*/true))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid metric family name '{}' in OpenMetrics line: {}", name, line);
    skipAsciiSpaces(sv, p);
    rest = String{sv.substr(p)};
}

/// `# EOF` may be followed by ASCII space/tab only.
bool isStrictEofLine(std::string_view line)
{
    static constexpr std::string_view prefix = "# EOF";
    if (!line.starts_with(prefix))
        return false;
    for (size_t i = prefix.size(); i < line.size(); ++i)
        if (line[i] != ' ' && line[i] != '\t')
            return false;
    return true;
}

/// After logical EOF, only blank ASCII lines are allowed until physical end-of-input.
void checkOnlyBlankLinesAfterEof(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        String tail;
        readStringUntilNewlineInto(tail, buf);
        if (!buf.eof())
            buf.ignore();
        if (!tail.empty() && tail.back() == '\r')
            tail.pop_back();
        for (char c : tail)
            if (c != ' ' && c != '\t')
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after # EOF in OpenMetrics input");
    }
}

/// String columns insert via `assert_cast<ColumnString &>`; reject FixedString / Nullable.
bool isPlainString(const DataTypePtr & t) { return !t->isNullable() && WhichDataType(t).isString(); }

/// `tags` accepts either `Map(String, String)` or `Array(Tuple(String, String))`.
bool isTagsColumnType(const DataTypePtr & type)
{
    if (isMap(type))
    {
        const auto * type_map = assert_cast<const DataTypeMap *>(type.get());
        return WhichDataType(type_map->getKeyType()).isString() && WhichDataType(type_map->getValueType()).isString();
    }
    if (isArray(type))
    {
        const auto * type_array = assert_cast<const DataTypeArray *>(type.get());
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
        return type_tuple && type_tuple->getElements().size() == 2
            && WhichDataType(type_tuple->getElement(0)).isString() && WhichDataType(type_tuple->getElement(1)).isString();
    }
    return false;
}

/// `samples` is inserted via `ColumnDecimal<DateTime64>` + `ColumnFloat64`, so require exactly
/// `Array(Tuple(DateTime64, Float64))`.
bool isSamplesColumnType(const DataTypePtr & type)
{
    if (!isArray(type))
        return false;
    const auto * type_array = assert_cast<const DataTypeArray *>(type.get());
    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    return type_tuple && type_tuple->getElements().size() == 2
        && isDateTime64(type_tuple->getElement(0)) && WhichDataType(type_tuple->getElement(1)).isFloat64();
}

void insertTags(IColumn & column, const std::vector<std::pair<String, String>> & tags)
{
    if (checkAndGetColumn<ColumnMap>(&column))
    {
        Field field = Map();
        Map & m = field.safeGet<Map>();
        for (const auto & [k, v] : tags)
            m.push_back(Tuple{k, v});
        column.insert(field);
    }
    else
    {
        Field field = Array();
        Array & a = field.safeGet<Array>();
        for (const auto & [k, v] : tags)
            a.push_back(Tuple{k, v});
        column.insert(field);
    }
}

void insertPoints(IColumn & column, const std::vector<std::pair<Int64, double>> & points, UInt32 scale)
{
    auto & col_array = assert_cast<ColumnArray &>(column);
    auto & col_tuple = assert_cast<ColumnTuple &>(col_array.getData());
    auto & col_ts = assert_cast<ColumnDecimal<DateTime64> &>(col_tuple.getColumn(0));
    auto & col_val = assert_cast<ColumnFloat64 &>(col_tuple.getColumn(1));
    for (const auto & [ms, value] : points)
    {
        Int64 raw = 0;
        if (!OpenMetricsText::tryRescaleDateTime64(ms, 3, scale, raw))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "OpenMetrics timestamp of {} ms does not fit DateTime64({})", ms, scale);
        col_ts.insertValue(DateTime64(raw));
        col_val.insertValue(value);
    }
    col_array.getOffsets().push_back(col_tuple.size());
}

}  /// anonymous namespace

OpenMetricsTextRowInputFormat::ColumnLoc OpenMetricsTextRowInputFormat::buildColumnLoc(const Block & header)
{
    using Pred = bool (*)(const DataTypePtr &);
    struct Spec
    {
        const char * name;
        std::optional<size_t> ColumnLoc::* slot;
        Pred pred;
    };
    /// All slots are optional: `markFormatSupportsSubsetOfColumns` lets `file()`/`format()` pass only
    /// the columns the query actually requests (e.g. `SELECT metric_name FROM file(..., OpenMetrics)`).
    /// The points column is `samples`, or `time_series` as in `TimeSeries` tables of version 2 and earlier.
    static const std::array<Spec, 8> specs = {{
        {"metric_name",   &ColumnLoc::metric_name,   &isPlainString},
        {"metric_family", &ColumnLoc::metric_family, &isPlainString},
        {"help",          &ColumnLoc::help,          &isPlainString},
        {"type",          &ColumnLoc::type,          &isPlainString},
        {"unit",          &ColumnLoc::unit,          &isPlainString},
        {"tags",          &ColumnLoc::tags,          &isTagsColumnType},
        {"samples",       &ColumnLoc::samples,       &isSamplesColumnType},
        {"time_series",   &ColumnLoc::samples,       &isSamplesColumnType},
    }};

    ColumnLoc loc;
    for (const auto & s : specs)
    {
        if (!header.has(s.name, /*case_insensitive=*/true))
            continue;
        const size_t idx = header.getPositionByName(s.name);
        const auto & col = header.getByPosition(idx);
        if (!s.pred(col.type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Illegal type '{}' of column '{}' for input format '{}'",
                col.type->getName(), s.name, FORMAT_NAME);
        if (loc.*(s.slot))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Columns 'samples' and 'time_series' cannot both be present for input format '{}'",
                FORMAT_NAME);
        loc.*(s.slot) = idx;
    }
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
    parsed = false;
    output_rows.clear();
    next_output_row = 0;
}

void OpenMetricsTextRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
}

String OpenMetricsTextRowInputFormat::deriveMetricFamily(const String & metric_name) const
{
    /// A name that carries its own `# HELP` / `# TYPE` / `# UNIT` is its own family, even if it also
    /// looks like a suffixed sample of some other family (e.g. an explicitly typed `foo_total`).
    const auto declared = [this](const String & name)
    {
        const auto it = family_meta.find(name);
        return it != family_meta.end() && (it->second.has_help || it->second.has_type || it->second.has_unit);
    };
    if (declared(metric_name))
        return metric_name;

    /// Fold a suffixed sample back to its base family when the base's declared type lists that suffix
    /// (OpenMetrics MetricType suffix rules, shared with the writer). The type stored in `family_meta`
    /// is already normalized (`untyped` -> `unknown`).
    for (const auto suffix : OpenMetricsText::ALL_FAMILY_SAMPLE_SUFFIXES)
    {
        if (metric_name.size() <= suffix.size() || !metric_name.ends_with(suffix))
            continue;
        const String base = metric_name.substr(0, metric_name.size() - suffix.size());
        const auto it = family_meta.find(base);
        if (it == family_meta.end())
            continue;
        const auto * suffixes = OpenMetricsText::familySampleSuffixes(it->second.type);
        if (suffixes && std::find(suffixes->begin(), suffixes->end(), suffix) != suffixes->end())
            return base;
    }
    return metric_name;
}

void OpenMetricsTextRowInputFormat::parseAll()
{
    /// OpenMetrics requires every `# HELP` / `# TYPE` / `# UNIT` to precede the family's samples: the
    /// metadata governs family membership (`_bucket` / `_sum` / `_count`, counter `_total`, and the
    /// `_created` / `_gcount` / `_gsum` / `_info` siblings), so a sample already emitted under any of
    /// them means the metadata is late and would make the parse order-dependent. Probe with `find` so
    /// checking siblings never grows the map.
    const auto familyOrSiblingEmittedSample = [this](const String & family) -> bool
    {
        const auto emitted = [this](const String & key)
        {
            const auto it = family_meta.find(key);
            return it != family_meta.end() && it->second.samples_emitted;
        };

        if (emitted(family))
            return true;
        for (const auto suffix : OpenMetricsText::ALL_FAMILY_SAMPLE_SUFFIXES)
            if (emitted(family + String{suffix}))
                return true;
        return false;
    };

    /// Maps a series identity (metric_name + tags) to its row in `output_rows`, so repeated samples of
    /// the same series accumulate into one `samples` array.
    std::unordered_map<String, size_t> series_index;

    while (!in->eof() && !saw_eof)
    {
        String line;
        readStringUntilNewlineInto(line, *in);
        if (!in->eof())
            in->ignore();
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        if (line.empty())
            continue;

        if (line.starts_with("#"))
        {
            if (line.starts_with("# EOF"))
            {
                if (!isStrictEofLine(line))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid # EOF line in OpenMetrics input");
                checkOnlyBlankLinesAfterEof(*in);
                saw_eof = true;
                continue;
            }

            String name;
            String rest;
            /// OpenMetrics allows at most one of each descriptor per family, all preceding its samples.
            if (line.starts_with("# HELP "))
            {
                parseMetadataLine(line, sizeof("# HELP ") - 1, name, rest);
                auto & fm = family_meta[name];
                if (familyOrSiblingEmittedSample(name))
                    throwIncorrect("'# HELP' metadata cannot follow samples for a metric family", line);
                if (fm.has_help)
                    throwIncorrect("Duplicate '# HELP' metadata for a metric family", line);
                fm.help = std::move(rest);
                fm.has_help = true;
            }
            else if (line.starts_with("# TYPE "))
            {
                parseMetadataLine(line, sizeof("# TYPE ") - 1, name, rest);
                /// Type is a single token; reject trailing data.
                size_t end = 0;
                while (end < rest.size() && rest[end] != ' ' && rest[end] != '\t')
                    ++end;
                if (end == 0)
                    throwIncorrect("Missing type in # TYPE descriptor", line);
                for (size_t i = end; i < rest.size(); ++i)
                    if (rest[i] != ' ' && rest[i] != '\t')
                        throwIncorrect("Unexpected trailing data in # TYPE descriptor", line);
                rest.resize(end);
                auto & fm = family_meta[name];
                if (familyOrSiblingEmittedSample(name))
                    throwIncorrect("'# TYPE' metadata cannot follow samples for a metric family", line);
                if (fm.has_type)
                    throwIncorrect("Duplicate '# TYPE' metadata for a metric family", line);
                /// Normalize to the OpenMetrics 1.0 vocabulary (Prometheus `untyped` -> `unknown`).
                fm.type = OpenMetricsText::normalizeOpenMetricsType(rest);
                if (!OpenMetricsText::isValidOpenMetricsType(fm.type))
                    throwIncorrect("Unknown metric type in # TYPE descriptor", line);
                fm.has_type = true;
            }
            else if (line.starts_with("# UNIT "))
            {
                parseMetadataLine(line, sizeof("# UNIT ") - 1, name, rest);
                auto & fm = family_meta[name];
                if (familyOrSiblingEmittedSample(name))
                    throwIncorrect("'# UNIT' metadata cannot follow samples for a metric family", line);
                if (fm.has_unit)
                    throwIncorrect("Duplicate '# UNIT' metadata for a metric family", line);
                fm.unit = std::move(rest);
                fm.has_unit = true;
            }
            /// Other `#` lines are free-form comments.
            continue;
        }

        std::string_view sv{line};
        size_t pos = 0;
        String stem;
        std::map<String, String> labels;
        parseMetricDescriptor(sv, pos, stem, labels, line);

        if (pos >= sv.size() || (sv[pos] != ' ' && sv[pos] != '\t'))
            throwIncorrect("Missing whitespace between metric descriptor and value", line);
        skipAsciiSpaces(sv, pos);

        const std::string_view value_tok = readToken(sv, pos);
        if (value_tok.empty())
            throwIncorrect("Cannot parse value", line);
        const Float64 value = parseSampleValue(value_tok);

        bool has_ts = false;
        std::string_view ts_token;
        skipAsciiSpaces(sv, pos);
        if (pos < sv.size() && sv[pos] != '#')
        {
            ts_token = readToken(sv, pos);
            /// Enforce the strict `realnumber` grammar (rejects NaN / Inf and Float64-overflowing
            /// tokens); the exact millisecond conversion below re-tokenizes for value.
            parseRealNumber(ts_token, line);
            has_ts = true;
        }

        parseExemplarSuffix(sv, pos, line);
        skipAsciiSpaces(sv, pos);
        if (pos < sv.size())
            throwIncorrect("Unexpected trailing data", line);

        /// A sample without an explicit timestamp is stored at epoch (millisecond 0); the on-wire
        /// name keeps its suffix, and the owning family (for metadata) is derived from `# TYPE`.
        const Int64 ms = has_ts ? secondsTokenToMillis(ts_token, line) : 0;

        const String metric_family = deriveMetricFamily(stem);
        static const FamilyMeta empty_meta;
        const auto meta_it = family_meta.find(metric_family);
        const FamilyMeta & fm = (meta_it == family_meta.end()) ? empty_meta : meta_it->second;

        /// Histogram buckets must carry `le` and bare summary samples `quantile` (the same rule the writer
        /// enforces), otherwise the sample is indistinguishable from an unrelated series.
        if (metric_family.size() <= stem.size() && stem.starts_with(metric_family))
        {
            const std::string_view suffix = std::string_view{stem}.substr(metric_family.size());
            if (const char * boundary = OpenMetricsText::requiredBoundaryLabel(fm.type, suffix);
                boundary && !labels.contains(boundary))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "OpenMetrics {} sample '{}' is missing the '{}' label", fm.type, stem, boundary);
        }

        /// `std::map` iterates in sorted label-name order, giving a deterministic, canonical tag order.
        std::vector<std::pair<String, String>> tags(labels.begin(), labels.end());

        String key = stem;
        key.push_back('\0');
        for (const auto & [k, v] : tags)
        {
            key += k;
            key.push_back('\0');
            key += v;
            key.push_back('\0');
        }

        const auto [it, inserted] = series_index.try_emplace(key, output_rows.size());
        if (inserted)
        {
            OutputSeries series;
            series.metric_name = stem;
            series.metric_family = metric_family;
            series.help = fm.help;
            series.type = fm.type;
            series.unit = fm.unit;
            series.tags = std::move(tags);
            series.points.emplace_back(ms, value);
            output_rows.push_back(std::move(series));
        }
        else
        {
            output_rows[it->second].points.emplace_back(ms, value);
        }

        /// Record that a sample has been emitted under this exact on-wire name so a later descriptor
        /// for the owning family is rejected (`operator[]` deliberately grows the map here).
        family_meta[stem].samples_emitted = true;
    }
}

bool OpenMetricsTextRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!column_loc_initialized)
    {
        const Block & header = getPort().getHeader();
        column_loc = buildColumnLoc(header);
        if (column_loc.samples)
        {
            const auto & ts_type = header.getByPosition(*column_loc.samples).type;
            const auto & ts_tuple = assert_cast<const DataTypeTuple &>(*assert_cast<const DataTypeArray &>(*ts_type).getNestedType());
            timestamp_scale = assert_cast<const DataTypeDateTime64 &>(*ts_tuple.getElement(0)).getScale();
        }
        column_loc_initialized = true;
    }

    if (!parsed)
    {
        parseAll();
        parsed = true;
    }

    /// Like JSONEachRow: empty `read_columns` signals "no row produced" rather than "all nulls".
    if (next_output_row >= output_rows.size())
    {
        ext.read_columns.clear();
        return false;
    }

    const OutputSeries & series = output_rows[next_output_row++];
    ext.read_columns.assign(columns.size(), 0);

    const auto set_string = [&](std::optional<size_t> idx, const String & str)
    {
        if (!idx)
            return;
        assert_cast<ColumnString &>(*columns[*idx]).insertData(str.data(), str.size());
        ext.read_columns[*idx] = 1;
    };

    set_string(column_loc.metric_name, series.metric_name);
    set_string(column_loc.metric_family, series.metric_family);
    set_string(column_loc.help, series.help);
    set_string(column_loc.type, series.type);
    set_string(column_loc.unit, series.unit);

    if (column_loc.tags)
    {
        insertTags(*columns[*column_loc.tags], series.tags);
        ext.read_columns[*column_loc.tags] = 1;
    }
    if (column_loc.samples)
    {
        insertPoints(*columns[*column_loc.samples], series.points, timestamp_scale);
        ext.read_columns[*column_loc.samples] = 1;
    }

    return true;
}

NamesAndTypesList OpenMetricsTextSchemaReader::readSchema()
{
    auto str = std::make_shared<DataTypeString>();
    auto tags = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{str, str}));
    auto samples = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeDateTime64>(3), std::make_shared<DataTypeFloat64>()}));
    return {
        {"metric_name",   str},
        {"metric_family", str},
        {"help",          str},
        {"type",          str},
        {"unit",          str},
        {"tags",          tags},
        {"samples",       samples},
    };
}

void registerInputFormatOpenMetrics(FormatFactory & factory);
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
