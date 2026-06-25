#include <Processors/Formats/Impl/OpenMetricsTextRowInputFormat.h>

#include <Processors/Formats/Impl/OpenMetricsText.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readFloatText.h>

#include <array>
#include <cmath>
#include <limits>
#include <map>


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
using OpenMetricsText::sampleKindCount;

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
    if (!tryReadFloatText(v, buf) || !buf.eof() || !std::isfinite(v))
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
    if (!tryReadFloatText(v, buf) || !buf.eof())
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
void parseMetadataLine(const String & line, size_t prefix_len, String & name, String & rest)
{
    std::string_view sv{line};
    size_t p = prefix_len;
    skipAsciiSpaces(sv, p);
    name = String{readToken(sv, p)};
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

bool isMapStringString(const DataTypePtr & type)
{
    if (!isMap(type))
        return false;
    const auto * m = assert_cast<const DataTypeMap *>(type.get());
    const auto & k = m->getKeyType();
    const auto & v = m->getValueType();
    /// Insertion uses `ColumnMap` materialized from `Map(String, String)`; reject FixedString / Nullable variants.
    return !k->isNullable() && !v->isNullable() && WhichDataType(k).isString() && WhichDataType(v).isString();
}

/// `value` is always inserted via `assert_cast<ColumnFloat64 &>`; reject `Nullable(Float64)`.
bool isPlainFloat64(const DataTypePtr & t) { return !t->isNullable() && WhichDataType(t).isFloat64(); }
/// String columns insert via `assert_cast<ColumnString &>`; reject FixedString / Nullable.
bool isPlainString(const DataTypePtr & t)  { return !t->isNullable() && WhichDataType(t).isString(); }
/// Timestamp can be `Int64` or `Nullable(Int64)`.
bool isOptionalInt64(const DataTypePtr & t)
{
    if (t->isNullable())
        return WhichDataType(assert_cast<const DataTypeNullable *>(t.get())->getNestedType()).isInt64();
    return WhichDataType(t).isInt64();
}

void insertMapLabels(IColumn & column, const std::map<String, String> & labels)
{
    Field map_field = Map();
    Map & m = map_field.safeGet<Map>();
    for (const auto & [k, v] : labels)
        m.push_back(Tuple{k, v});
    column.insert(map_field);
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
    /// All slots are optional: `markFormatSupportsSubsetOfColumns` lets `file()`/`format()`
    /// pass only the columns the query actually requests (e.g. `SELECT name FROM file(..., OpenMetrics)`).
    /// `readRow` still parses the `name` and `value` tokens on every line because OpenMetrics
    /// requires them; it just skips the insertion when the slot is absent.
    static const std::array<Spec, 7> specs = {{
        {"name",      &ColumnLoc::name,      &isPlainString},
        {"value",     &ColumnLoc::value,     &isPlainFloat64},
        {"help",      &ColumnLoc::help,      &isPlainString},
        {"type",      &ColumnLoc::type,      &isPlainString},
        {"labels",    &ColumnLoc::labels,    &isMapStringString},
        {"timestamp", &ColumnLoc::timestamp, &isOptionalInt64},
        {"unit",      &ColumnLoc::unit,      &isPlainString},
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

    /// Like JSONEachRow: empty `read_columns` signals "no row produced" rather than "all nulls" (Code 7).
    ext.read_columns.clear();

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
            /// OpenMetrics allows at most one `# HELP` / `# TYPE` / `# UNIT` per metric family;
            /// a duplicate descriptor would silently overwrite the first and make the parsed rows
            /// order-dependent, so reject it.
            if (line.starts_with("# HELP "))
            {
                parseMetadataLine(line, sizeof("# HELP ") - 1, name, rest);
                auto & fm = family_meta[name];
                if (fm.has_help)
                    throwIncorrect("Duplicate '# HELP' metadata for a metric family", line);
                fm.help = std::move(rest);
                fm.has_help = true;
            }
            else if (line.starts_with("# TYPE "))
            {
                parseMetadataLine(line, sizeof("# TYPE ") - 1, name, rest);
                /// Type is a single token (counter/gauge/histogram/summary/untyped).
                size_t end = 0;
                while (end < rest.size() && rest[end] != ' ' && rest[end] != '\t')
                    ++end;
                if (end == 0)
                    throwIncorrect("Missing type in # TYPE descriptor", line);
                for (size_t i = end; i < rest.size(); ++i)
                {
                    if (rest[i] != ' ' && rest[i] != '\t')
                        throwIncorrect("Unexpected trailing data in # TYPE descriptor", line);
                }
                rest.resize(end);
                auto & fm = family_meta[name];
                if (fm.has_type)
                    throwIncorrect("Duplicate '# TYPE' metadata for a metric family", line);
                fm.type = std::move(rest);
                fm.has_type = true;
            }
            else if (line.starts_with("# UNIT "))
            {
                parseMetadataLine(line, sizeof("# UNIT ") - 1, name, rest);
                auto & fm = family_meta[name];
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
        Float64 ts_value = 0;
        std::string_view ts_token;
        skipAsciiSpaces(sv, pos);
        if (pos < sv.size() && sv[pos] != '#')
        {
            ts_token = readToken(sv, pos);
            ts_value = parseRealNumber(ts_token, line);
            has_ts = true;
        }

        parseExemplarSuffix(sv, pos, line);
        skipAsciiSpaces(sv, pos);
        if (pos < sv.size())
            throwIncorrect("Unexpected trailing data", line);

        std::optional<Int64> parsed_timestamp_ms;
        if (has_ts)
            parsed_timestamp_ms = secondsTokenToMillis(ts_token, ts_value, line);

        /// Fold `_bucket`/`_sum`/`_count` siblings into their `# TYPE` family (see SUFFIX_RULES for
        /// the type-specific contract). Folding a suffix synthesizes the empty marker label
        /// (`labels[sum]=""` / `labels[count]=""`); a pre-existing non-empty label of that name is a
        /// collision and is rejected rather than silently overwritten.
        String logical_name = stem;
        for (const auto & r : OpenMetricsText::SUFFIX_RULES)
        {
            if (!stem.ends_with(r.suffix) || stem.size() <= r.suffix.size())
                continue;
            const String base = stem.substr(0, stem.size() - r.suffix.size());
            auto it = family_meta.find(base);
            if (it == family_meta.end())
                break;
            const String & base_type = it->second.type;
            const bool type_matches = r.histogram_only
                ? (base_type == "histogram")
                : (base_type == "histogram" || base_type == "summary");
            if (!type_matches)
                break;
            if (r.suffix == "_bucket" && !labels.contains("le"))
                throwIncorrect("Histogram bucket sample is missing required 'le' label", line);
            logical_name = base;
            if (!r.synth_label.empty())
            {
                const String synth_key(r.synth_label);
                if (auto label_it = labels.find(synth_key); label_it != labels.end() && !label_it->second.empty())
                    throwIncorrect(
                        fmt::format(
                            "Sample line for family '{}' uses suffix '{}' but its labels already contain '{}=\"{}\"'",
                            base, r.suffix, synth_key, label_it->second),
                        line);
                labels[synth_key] = "";
            }
            break;
        }

        /// A `counter` family `foo` exposes its sample as `foo_total`. Unlike the histogram/summary
        /// siblings folded above, the `_total` suffix is NOT dropped: the emitted name stays
        /// `foo_total`, but the row inherits the base family's type/help/unit. Decouple the
        /// metadata-family lookup from `logical_name` so the name round-trips unchanged while the
        /// metadata comes from the base family.
        String meta_family_name = logical_name;
        {
            static constexpr std::string_view total_suffix = "_total";
            if (stem.ends_with(total_suffix) && stem.size() > total_suffix.size() && !family_meta.contains(stem))
            {
                const String base = stem.substr(0, stem.size() - total_suffix.size());
                if (auto it = family_meta.find(base); it != family_meta.end() && it->second.type == "counter")
                    meta_family_name = base;
            }

            /// Sibling suffixes ClickHouse does not model: reject them when the base is a declared
            /// family, but leave a standalone `x_created` (no declared base family) to ingest as an
            /// ordinary metric named `x_created`.
            static constexpr std::array<std::string_view, 3> unsupported_suffixes = {"_created", "_gcount", "_gsum"};
            for (const auto & suffix : unsupported_suffixes)
            {
                if (!stem.ends_with(suffix) || stem.size() <= suffix.size())
                    continue;
                const String base = stem.substr(0, stem.size() - suffix.size());
                if (family_meta.contains(base))
                    throwIncorrect(
                        fmt::format("Unsupported OpenMetrics sibling suffix '{}' for family '{}'", suffix, base),
                        line);
                break;
            }
        }

        /// Don't `operator[]` on lookup — that would grow the map for every unseen family name.
        static const FamilyMeta empty_meta;
        const auto meta_it = family_meta.find(meta_family_name);
        const FamilyMeta & fm = (meta_it == family_meta.end()) ? empty_meta : meta_it->second;

        if (fm.type == "histogram" || fm.type == "summary")
        {
            /// A histogram/summary sample must carry exactly one sample kind: a bucket/quantile
            /// sample, or a `_sum` / `_count` marker. Zero kinds (e.g. `# TYPE h histogram` followed
            /// by a bare `h 3`) and more than one (combined kinds) are both invalid exposition.
            const size_t kinds = sampleKindCount(fm.type, labels);
            if (kinds != 1)
                throwIncorrect(
                    fmt::format(
                        "Sample for family '{}' with type '{}' must carry exactly one histogram/summary sample kind "
                        "(a bucket/quantile sample or a '_sum' / '_count' marker), but has {}",
                        logical_name, fm.type, kinds),
                    line);
        }

        ext.read_columns.assign(columns.size(), 0);

        const auto setString = [&](std::optional<size_t> idx, std::string_view str)
        {
            if (!idx)
                return;
            assert_cast<ColumnString &>(*columns[*idx]).insertData(str.data(), str.size());
            ext.read_columns[*idx] = 1;
        };

        setString(loc.name, logical_name);

        if (loc.value)
        {
            assert_cast<ColumnFloat64 &>(*columns[*loc.value]).insert(value);
            ext.read_columns[*loc.value] = 1;
        }

        setString(loc.help, fm.help);
        setString(loc.type, fm.type);

        if (loc.labels)
        {
            insertMapLabels(*columns[*loc.labels], labels);
            ext.read_columns[*loc.labels] = 1;
        }

        if (loc.timestamp)
        {
            auto & col = *columns[*loc.timestamp];
            if (!has_ts)
            {
                if (!col.isNullable())
                    throwIncorrect("Timestamp column is not Nullable but line has no timestamp", line);
                assert_cast<ColumnNullable &>(col).insertDefault();
            }
            else
            {
                const Int64 t = *parsed_timestamp_ms;
                if (col.isNullable())
                {
                    auto & nc = assert_cast<ColumnNullable &>(col);
                    nc.getNestedColumn().insert(t);
                    nc.getNullMapColumn().insertValue(0);
                }
                else
                    assert_cast<ColumnInt64 &>(col).insert(t);
            }
            ext.read_columns[*loc.timestamp] = 1;
        }

        setString(loc.unit, fm.unit);

        return true;
    }

    ext.read_columns.clear();
    return false;
}

NamesAndTypesList OpenMetricsTextSchemaReader::readSchema()
{
    return {
        {"name",      std::make_shared<DataTypeString>()},
        {"value",     std::make_shared<DataTypeFloat64>()},
        {"help",      std::make_shared<DataTypeString>()},
        {"type",      std::make_shared<DataTypeString>()},
        {"labels",    std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        {"timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"unit",      std::make_shared<DataTypeString>()},
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
