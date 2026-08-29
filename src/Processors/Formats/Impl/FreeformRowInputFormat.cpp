#include <Processors/Formats/Impl/FreeformRowInputFormat.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <base/sort.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/EscapingRuleUtils.h>
#include <Common/Documentation.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int UNSUPPORTED_METHOD;
}

/// Skips separators inside a row and deliberately stops at a line break: skipping `\n`/`\r`
/// here would let a row that ends with a trailing delimiter silently continue on the next
/// physical line, stitching two rows into one.
static inline void skipWhitespacesAndDelimiters(ReadBuffer & in)
{
    while (!in.eof() && (isWhitespaceASCIIOneLine(*in.position()) || *in.position() == ',' || *in.position() == ':'))
        ++in.position();
}

/// A row ends at the end of the data or at a line break.
static inline bool atRowEnd(ReadBuffer & in)
{
    return in.eof() || *in.position() == '\n' || *in.position() == '\r';
}

// Returns the score of the given type. This doesn't take nullable into account.
// Possible return values (increasing by power of 5):
// - 1: String
// - 5: Decimal, Float
// - 25: Map, Array, Tuple
// - 125: Date, DateTime,
static unsigned scoreForType(const DataTypePtr & type)
{
    WhichDataType which(type);

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return scoreForType(nullable_type->getNestedType());
    }

    if (which.isDateOrDate32() || which.isDateTimeOrDateTime64())
        return 125;

    if (which.isMap() || which.isArray() || which.isTuple())
        return 25;

    if (which.isDecimal() || which.isFloat() || which.isInt() || which.isUInt())
        return 5;

    return 1;
}

static unsigned scoreForRule(FormatSettings::EscapingRule rule)
{
    switch (rule)
    {
        case FormatSettings::EscapingRule::JSON:
            return 4;
        case FormatSettings::EscapingRule::CSV:
            [[fallthrough]];
        case FormatSettings::EscapingRule::Quoted:
            return 2;
        case FormatSettings::EscapingRule::Escaped:
            [[fallthrough]];
        case FormatSettings::EscapingRule::Raw:
            return 1;
        default:
            return 0;
    }
    UNREACHABLE();
}

static unsigned scoreForField(FormatSettings::EscapingRule rule, const DataTypePtr & type)
{
    return scoreForRule(rule) * scoreForType(type);
}

static FieldMatcher::Result makeFailedResult()
{
    return {
        .names_and_types = {},
        .fields = {},
        .score = 0,
        .type_score = 0,
        .offset = 0,
        .ok = false,
        .parse_till_newline_as_one_string = false,
    };
}

FieldMatcher::Result FieldMatcher::generateResult(NamesAndFields & fields, size_t offset)
{
    if (fields.empty())
        return makeFailedResult();

    NamesAndTypesList names_and_types;
    std::vector<String> values;
    unsigned type_score = 0;
    unsigned score = 0;
    for (auto & [col, field] : fields)
    {
        /// An empty field or a field consisting of a single delimiter is a sign that the matcher
        /// consumed a separator, not data. Only the actual delimiter set is rejected here: other
        /// one-character punctuation tokens (a `-` placeholder in Apache or syslog logs) are data.
        if (field.empty() || (field.size() == 1 && (field[0] == ',' || field[0] == ':' || isWhitespaceASCII(field[0]))))
            return makeFailedResult();

        auto type = getDataTypeFromField(field);
        if (!type)
            return makeFailedResult();

        names_and_types.emplace_back(col, type);
        values.emplace_back(field);
        score += scoreForField(getEscapingRule(), type);
        type_score += scoreForType(type);
    }

    return {
        .names_and_types = names_and_types,
        .fields = values,
        .score = score,
        .type_score = type_score,
        .offset = offset,
        .ok = true,
        .parse_till_newline_as_one_string = (fields.rbegin()->second.ends_with(':') && getName() == "RawByWhitespaceFieldMatcher"),
    };
}

template <bool with_offset>
FieldMatcher::Result FieldMatcher::parseField(PeekableReadBuffer & in, unsigned index)
{
    try
    {
        /// The buffer may be exhausted here: trailing whitespace and delimiters are consumed
        /// before a matcher is tried. Dereferencing the position of an exhausted buffer would
        /// read uninitialized memory beyond the last byte of the data. A matcher invoked at a
        /// line break must likewise fail: a field never continues past the end of the row.
        if (atRowEnd(in))
            return makeFailedResult();

        auto fields = readFieldsByEscapingRule(in, index);

        /// A matcher has to consume the whole field: if it stopped in the middle of a token
        /// (as a JSON matcher does on `2022.11.17`, reading it as the number `2022.11`),
        /// the rest of the row would be parsed at a shifted position.
        if (!in.eof() && !isWhitespaceASCII(*in.position()) && *in.position() != ',' && *in.position() != ':')
            return makeFailedResult();

        if constexpr (with_offset)
            return generateResult(fields, in.offsetFromCheckpoint()); // offset is not needed if this is called in parseRow()

        return generateResult(fields, 0);
    }
    catch (Exception & e)
    {
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "Error while parsing: {}", e.message());
        return makeFailedResult();
    }
}

FieldMatcher::NamesAndFields JSONFieldMatcher::readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const
{
    // If there's no opening bracket, read the field as a json field with the column name as c{index}
    if (*in.position() != '{')
    {
        String field;
        readJSONField(field, in, settings.json);
        return {{fmt::format("c{}", index), field}};
    }

    // Else, attempt to parse each JSON fields of the root object as separate columns.

    ++in.position();

    skipWhitespacesAndDelimiters(in);
    NamesAndFields cols_and_fields;
    while (true)
    {
        /// The object may be truncated by the end of the data, and the position of an exhausted
        /// buffer must not be dereferenced.
        if (in.eof())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected end of stream while parsing a JSON object");

        if (*in.position() == '}')
            break;

        String col = JSONUtils::readFieldName(in, settings.json);
        String field;

        if (!in.eof() && *in.position() == '{')
            readJSONObjectPossiblyInvalid(field, in);
        else
            readJSONField(field, in, settings.json);

        cols_and_fields.emplace_back(col, field);
        skipWhitespacesAndDelimiters(in);
    }

    ++in.position();

    return cols_and_fields;
}

FieldMatcher::NamesAndFields CSVFieldMatcher::readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const
{
    String field;
    readCSVField(field, in, settings.csv);
    return {{fmt::format("c{}", index), field}};
}

FieldMatcher::NamesAndFields QuotedFieldMatcher::readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const
{
    String field;
    readQuotedField(field, in);
    return {{fmt::format("c{}", index), field}};
}

FieldMatcher::NamesAndFields EscapedFieldMatcher::readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const
{
    String field;
    readEscapedString(field, in);
    return {{fmt::format("c{}", index), field}};
}

FieldMatcher::NamesAndFields RawByWhitespaceFieldMatcher::readFieldsByEscapingRule(PeekableReadBuffer & in, unsigned index) const
{
    String field;
    readStringUntilWhitespaceDelimiter(field, in);
    return {{fmt::format("c{}", index), field}};
}

FreeformFieldMatcher::FreeformFieldMatcher(ReadBuffer & in_, const FormatSettings & settings_)
    : max_rows_to_check(std::min<size_t>(100, settings_.max_rows_to_read_for_schema_inference))
    , in(std::make_unique<PeekableReadBuffer>(in_))
{
    // matchers are pushed in the order of priority, this helps with exiting early and reducing the search tree.
    matchers.emplace_back(std::make_unique<JSONFieldMatcher>(FormatSettings::EscapingRule::JSON, settings_));
    matchers.emplace_back(std::make_unique<CSVFieldMatcher>(FormatSettings::EscapingRule::CSV, settings_));
    matchers.emplace_back(std::make_unique<RawByWhitespaceFieldMatcher>(FormatSettings::EscapingRule::Raw, settings_));
    matchers.emplace_back(std::make_unique<QuotedFieldMatcher>(FormatSettings::EscapingRule::Quoted, settings_));
    matchers.emplace_back(std::make_unique<EscapedFieldMatcher>(FormatSettings::EscapingRule::Escaped, settings_));
}

void FreeformFieldMatcher::seekInRow(size_t offset) const
{
    /// A single checkpoint is kept at the beginning of the row, and every position inside the row is
    /// addressed by its offset from it. Nested checkpoints are deliberately not used here: the search
    /// rewinds the buffer over and over, and a single checkpoint keeps the bookkeeping trivial.
    in->rollbackToCheckpoint();
    in->ignore(offset);
}

std::vector<FreeformFieldMatcher::Fields>
FreeformFieldMatcher::readNextFields(bool parse_till_newline_as_one_string, unsigned index, size_t offset) const
{
    std::vector<Fields> next_fields;
    if (parse_till_newline_as_one_string)
    {
        auto result = matchers.back()->parseField<true>(*in, index);
        if (result.ok)
            next_fields.emplace_back(result, matchers.size() - 1);

        return next_fields;
    }

    size_t best_score = 0;
    for (uint8_t i = 0; const auto & matcher : matchers)
    {
        auto result = matcher->parseField<true>(*in, index);
        if (result.ok)
        {
            // best_score <= 1 means that we've only found strings so far, in that case it's best to include all of the fields
            if (best_score <= 1 || result.type_score > best_score)
            {
                best_score = result.type_score;
                next_fields.emplace_back(result, i);
            }
        }

        ++i;
        seekInRow(offset);
    }

    return next_fields;
}

void FreeformFieldMatcher::buildSolutions(
    Solution current_solution, std::vector<Solution> & solutions, bool parse_till_newline_as_one_string, size_t offset) const
{
    seekInRow(offset);
    skipWhitespacesAndDelimiters(*in);

    /// Trailing delimiters before a line break end the row too: delimiter runs are collapsed
    /// by design, and the alternative of walking into the next physical line would stitch
    /// two rows into one.
    if (atRowEnd(*in))
    {
        solutions.push_back(current_solution);
        return;
    }

    const size_t offset_after_delimiters = in->offsetFromCheckpoint();

    const auto next_fields = readNextFields(parse_till_newline_as_one_string, current_solution.size, offset_after_delimiters);
    for (const auto & fields : next_fields)
    {
        auto next = current_solution;
        next.matchers_order.push_back(fields.matcher_index);
        for (const auto & name_and_type : fields.parse_result.names_and_types)
            next.columns.push_back(name_and_type);

        next.score += fields.parse_result.score;
        next.size += fields.parse_result.names_and_types.size();

        buildSolutions(next, solutions, fields.parse_result.parse_till_newline_as_one_string, fields.parse_result.offset);
    }
}

bool FreeformFieldMatcher::validateSolution(Solution solution) const
{
    try
    {
        // A map mapping column name to an index. This allows transforming multiple rows in one columns into one type.
        std::unordered_map<String, unsigned> column_index;
        for (unsigned i = 0; const auto & [name, _] : solution.columns)
        {
            /// A duplicate name (a root JSON key colliding with an autogenerated `cN` name, or a
            /// duplicate key) would make two columns share one index, so one of them would silently
            /// take the values of the other.
            if (column_index.contains(name))
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Duplicate column name '{}' in the inferred solution", name);

            column_index[name] = i++;
        }

        for (size_t row = 0; row < max_rows_to_check; ++row)
        {
            // For each iteration, we try to parse fields and find the type union of the current row and the previously parsed rows. If it doesn't exist,
            // the solution is invalid. Additionally, we also try to check if for each row, we're getting the right number of columns and if we're ending
            // each row at \n
            if (in->eof())
                break;


            unsigned validated_columns = 0;
            for (const auto & i : solution.matchers_order)
            {
                skipWhitespacesAndDelimiters(*in);
                auto result = matchers[i]->parseField<false>(*in, validated_columns);
                if (!result.ok)
                    break;

                for (const auto & name_and_type : result.names_and_types)
                {
                    auto type = name_and_type.type;
                    auto name = name_and_type.name;
                    if (!type || !column_index.contains(name))
                        break;

                    auto type_index = column_index[name];
                    matchers[i]->transformTypesIfPossible(solution.columns[type_index].type, type);
                    if (!solution.columns[type_index].type->equals(*type))
                        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Received unexpected type after transform attempt");

                    ++validated_columns;
                }
            }

            if (validated_columns < solution.columns.size())
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unable to parse the desired number of fields");

            skipWhitespacesAndDelimiters(*in);
            if (!atRowEnd(*in))
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Solution did not end at newline character");

            skipToNextLineOrEOF(*in);
        }

        in->rollbackToCheckpoint();
        return true;
    }
    catch (Exception & e)
    {
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "Solution fails: {}", e.message());
    }

    in->rollbackToCheckpoint();
    return false;
}

bool FreeformFieldMatcher::buildSolutionsAndPickBest()
{
    if (!final_solution.matchers_order.empty())
        // if a solution is found already, we could return immediately
        // this is useful in the case of readRow
        //
        // temporary solution until we could reuse the solution generated in readSchema,
        // possibly by making use of the SchemaCache
        return true;

    skipBOMIfExists(*in);
    if (in->eof())
        return false;

    in->setCheckpoint();

    std::vector<Solution> solutions;
    buildSolutions(Solution{.columns = {}, .matchers_order = {}, .score = 0, .size = 0}, solutions, false, 0);
    in->rollbackToCheckpoint();
    if (solutions.empty())
    {
        in->rollbackToCheckpoint(true);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty solutions set");
    }

    ::sort(
        solutions.begin(),
        solutions.end(),
        [](const Solution & first, const Solution & second)
        { return std::tie(first.score, first.matchers_order) > std::tie(second.score, second.matchers_order); });

    // after finding and ranking the solutions, we now run them through the max_rows_to_check rows and pick the first one that works for all of them
    for (const auto & solution : solutions)
        if (validateSolution(solution))
        {
            final_solution = solution;
            in->rollbackToCheckpoint(true);
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "Found solution");
            return true;
        }

    in->rollbackToCheckpoint(true);
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "None of the {} candidate solutions parses every checked row", solutions.size());
}

bool FreeformFieldMatcher::parseRow()
{
    /// Blank lines and whitespace at the end of the data are not rows, but a delimiter here
    /// would belong to a row, so only whitespace is skipped.
    skipWhitespaceIfAny(*in);
    if (in->eof() || final_solution.matchers_order.empty())
        return false;

    /// Every field is reassigned below; a row that stops matching the solution must fail
    /// instead of silently reusing the previous row's values.
    matched_fields.assign(final_solution.size, {});
    rules.resize(final_solution.size);

    unsigned assigned_fields = 0;
    for (unsigned col{0}; const auto & i : final_solution.matchers_order)
    {
        skipWhitespacesAndDelimiters(*in);
        auto result = matchers[i]->parseField<false>(*in, col);
        if (!result.ok)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Row does not match the inferred solution: cannot parse a field with {}",
                matchers[i]->getName());

        for (size_t j = 0; const auto & [name, _] : result.names_and_types)
        {
            if (!first_row)
            {
                /// A key not seen in the first row (an extra key of a JSON object) is skipped,
                /// but `j` still advances: `fields` is parallel to `names_and_types`, and skipping
                /// a value would shift every following field of this matcher by one.
                auto it = field_name_to_index.find(name);
                if (it != field_name_to_index.end())
                {
                    matched_fields[it->second] = result.fields[j];
                    ++assigned_fields;
                }
            }
            else
            {
                if (col >= final_solution.size)
                    throw Exception(
                        ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Row has more fields than the {} fields of the inferred solution",
                        final_solution.size);

                /// See the identical check in `validateSolution`: two columns must not share a name.
                if (field_name_to_index.contains(name))
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Duplicate column name '{}' in the inferred solution", name);

                field_name_to_index[name] = col;
                rules[col] = matchers[i]->getEscapingRule();
                matched_fields[col] = result.fields[j];
                ++assigned_fields;
            }

            ++j;
            ++col;
        }
    }

    if (assigned_fields < final_solution.size)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Row matched only {} out of {} fields of the inferred solution",
            assigned_fields,
            final_solution.size);

    skipWhitespacesAndDelimiters(*in);
    if (!atRowEnd(*in))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Row does not end at a newline after all fields of the inferred solution");

    first_row = false;
    skipToNextLineOrEOF(*in);
    return true;
}

FreeformRowInputFormat::FreeformRowInputFormat(
    ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), format_settings(format_settings_), matcher(in_, format_settings_)
{
}

bool FreeformRowInputFormat::readField(unsigned index, MutableColumns & columns)
{
    const auto & type = matcher.getNamesAndTypes()[index].type;
    const auto rule = matcher.getRule(index);
    ReadBufferFromString field_buf(matcher.getField(index));

    return deserializeFieldByEscapingRule(type, serializations[index], *columns[index], field_buf, rule, format_settings);
}

bool FreeformRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    /// `buildSolutionsAndPickBest` returns false only for empty input (any other failure throws
    /// there), and the caller has already provided the structure, so an empty input is simply
    /// zero rows, as in every other input format. Only schema inference has to fail on it.
    if (!matcher.buildSolutionsAndPickBest())
        return false;

    if (matcher.parseRow())
    {
        auto size = matcher.getSolutionLength();

        /// `columns` and `serializations` are built from the header the caller provided
        /// (an explicit structure of `file(...)` or `INSERT ... FORMAT Freeform`), and the
        /// inferred solution is not obliged to fit it.
        if (size != columns.size())
            throw Exception(
                ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
                "The inferred solution has {} fields per row, while {} columns are expected",
                size,
                columns.size());

        ext.read_columns.assign(size, false);
        for (unsigned index = 0; index < size; ++index)
            ext.read_columns[index] = readField(index, columns);
    }

    return !in->eof();
}

void FreeformRowInputFormat::syncAfterError()
{
    skipToNextLineOrEOF(*in);
    // This might be problematic as the next \n is not guaranteed to be the next row
}

FreeformSchemaReader::FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowSchemaReader(in_, format_settings_), matcher(in_, format_settings_)
{
}

NamesAndTypesList FreeformSchemaReader::readSchema()
{
    if (!matcher.buildSolutionsAndPickBest())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse freeform text, no solutions found");

    auto columns = matcher.getNamesAndTypes();
    NamesAndTypesList ret;
    for (const auto & column : columns)
        ret.push_back(column);

    return ret;
}

std::optional<DataTypes> FreeformSchemaReader::readRowAndGetDataTypes()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "This method is not used and unimplemented for Freeform schema inference");
}

void registerInputFormatFreeform(FormatFactory & factory);
void registerInputFormatFreeform(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Freeform",
        [](ReadBuffer & buf, const Block & header, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<FreeformRowInputFormat>(buf, std::make_shared<const Block>(header), params, settings); });

    factory.setDocumentation("Freeform", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

The `Freeform` format reads tabular text whose escaping rules are not known in advance, such as
delimiter-separated files that are almost, but not quite, `CSV`, or space-separated files with an
arbitrary quoting style. Unlike [`Template`](/interfaces/formats/Template), it needs no format
string: the structure is inferred from the data.

Every field of the first row is parsed with each of the supported escaping rules - `JSON`, `CSV`,
raw (up to the next whitespace), `Quoted` and `Escaped` - and each sequence of rules that parses
the whole row is a candidate *solution*. A solution is scored by the escaping rule and by the type
of every field it produces, so that the more specific a type is, the higher it scores: a date or a
date-time scores above a container, which scores above a number, which scores above a string. The
solutions are then tried in the order of their score against up to 100 rows, and the first one that
parses all of them and infers a consistent type for every column wins.

Column names come from the data where the escaping rule provides them - a `JSON` object contributes
one column per key, named after that key - and are `c0`, `c1`, ... otherwise. A JSON object is
inferred as a `Map`, not as a named `Tuple`, because consecutive rows of a freeform file are free to
carry a different set of keys.

## Example usage {#example-usage}

Reading a file whose format is not known:

```sql
SELECT * FROM file('access.log', 'Freeform')
```

Inspecting the inferred structure:

```sql
DESCRIBE file('access.log', 'Freeform')
```

## Format settings {#format-settings}

The number of rows the candidate solutions are checked against is
[`input_format_max_rows_to_read_for_schema_inference`](/operations/settings/formats#input_format_max_rows_to_read_for_schema_inference),
capped at 100.

:::note
This format expects the input to be *tabular*: every row has to have the same structure. A file
mixing many differently-shaped messages - the general log-parsing problem solved by algorithms such
as Drain and Brain - has no single solution, and no schema is inferred for it.
:::
)DOCS_MD",
    });
}

void registerFreeformSchemaReader(FormatFactory & factory);
void registerFreeformSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Freeform",
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<FreeformSchemaReader>(buf, settings); });
}
}
