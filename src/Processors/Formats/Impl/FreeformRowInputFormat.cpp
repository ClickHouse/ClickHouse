#include "Processors/Formats/Impl/FreeformRowInputFormat.h"
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include "Common/Exception.h"
#include "Common/StringUtils/StringUtils.h"
#include <Common/logger_useful.h>
#include "Core/NamesAndTypes.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatFactory.h"
#include "Formats/JSONUtils.h"
#include "IO/ReadHelpers.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/ISchemaReader.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNSUPPORTED_METHOD;
}

static inline void skipWhitespacesAndDelimiters(ReadBuffer & in)
{
    while (!in.eof() && (isWhitespaceASCII(*in.position()) || *in.position() == ',' || *in.position() == ':'))
        ++in.position();
}

// Returns the score of the given type. This doesn't take nullable into account.
// Possible return values (increasing by power of 5):
// - 1: String
// - 5: Decimal, Float
// - 25: Map, Array, Tuple
// - 125: Date, DateTime,
static size_t scoreForType(const DataTypePtr & type)
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

static size_t scoreForRule(FormatSettings::EscapingRule rule)
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

static size_t scoreForField(FormatSettings::EscapingRule rule, const DataTypePtr & type)
{
    return scoreForRule(rule) * scoreForType(type);
}

FieldMatcher::Result FieldMatcher::generateResult(String field, size_t offset, size_t index)
{
    auto type = getDataTypeFromField(field);
    if (!type)
    {
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "got null type from field: {}", field);
        return {.ok = false};
    }

    return {
        .name_and_type = {fmt::format("c{}", index), type},
        .field = field,
        .score = scoreForField(getEscapingRule(), type),
        .type_score = scoreForType(type),
        .offset = offset,
        .ok = !(field.size() <= 1 && isPunctuationASCII(field[0])),
        .parse_till_newline_as_one_string = (field.ends_with(':') && getName() == "RawByWhitespaceFieldMatcher"),
    };
}

FieldMatcher::Result FieldMatcher::parseField(PeekableReadBuffer & in, size_t index)
{
    try
    {
        auto field = readFieldByEscapingRule(in);
        return generateResult(field, in.offsetFromLastCheckpoint(), index);
    }
    catch (Exception & e)
    {
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "Error while parsing: {}", e.message());
        return {.ok = false};
    }
}

String JSONFieldMatcher::readFieldByEscapingRule(PeekableReadBuffer & in) const
{
    String field;
    readJSONField(field, in);
    return field;
}

String CSVFieldMatcher::readFieldByEscapingRule(PeekableReadBuffer & in) const
{
    String field;
    readCSVField(field, in, settings.csv);
    return field;
}

String QuotedFieldMatcher::readFieldByEscapingRule(PeekableReadBuffer & in) const
{
    String field;
    readQuotedField(field, in);
    return field;
}

String EscapedFieldMatcher::readFieldByEscapingRule(PeekableReadBuffer & in) const
{
    String field;
    readEscapedString(field, in);
    return field;
}

String RawByWhitespaceFieldMatcher::readFieldByEscapingRule(PeekableReadBuffer & in) const
{
    String field;
    readStringUntilWhitespaceDelimiter(field, in);
    return field;
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

std::vector<FreeformFieldMatcher::Fields> FreeformFieldMatcher::readNextFields(bool parse_till_newline_as_one_string, size_t index) const
{
    skipWhitespacesAndDelimiters(*in);
    LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "starting char: {}", *in->position());
    std::vector<Fields> next_fields;

    if (parse_till_newline_as_one_string)
    {
        auto result = matchers.back()->parseField(*in, index);
        if (result.ok)
        {
            // LOG_DEBUG(
            //     &Poco::Logger::get("FreeformFieldMatcher"), "field: {}, type: {}", result.field, result.name_and_type.type->getName());
            next_fields.emplace_back(result, matchers.size() - 1);
        }

        in->rollbackToCheckpoint();
        return next_fields;
    }

    size_t best_score = 0;
    for (size_t i = 0; const auto & matcher : matchers)
    {
        auto result = matcher->parseField(*in, index);
        if (result.ok)
        {
            // LOG_DEBUG(
            //     &Poco::Logger::get("FreeformFieldMatcher"), "field: {}, type: {}", result.field, result.name_and_type.type->getName());
            if (best_score <= 1 || result.type_score > best_score)
            {
                best_score = result.type_score;
                next_fields.emplace_back(result, i);
            }
        }

        ++i;
        in->rollbackToCheckpoint();
    }

    return next_fields;
}

void FreeformFieldMatcher::buildSolutions(
    Solution current_solution, std::vector<Solution> & solutions, bool parse_till_newline_as_one_string) const
{
    if (in->eof() || *in->position() == '\n')
    {
        solutions.push_back(current_solution);
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "solution size: {}", solutions.size());
        return;
    }

    in->setCheckpoint();
    const auto next_fields = readNextFields(parse_till_newline_as_one_string, current_solution.size);
    for (const auto & field : next_fields)
    {
        auto next = current_solution;
        next.matchers_order.push_back(field.matcher_index);
        next.columns.push_back(field.parse_result.name_and_type);

        next.score += field.parse_result.score;
        next.size += 1;

        in->ignore(field.parse_result.offset);
        buildSolutions(next, solutions, field.parse_result.parse_till_newline_as_one_string);
        in->rollbackToCheckpoint();
    }

    in->dropCheckpoint();
}

bool FreeformFieldMatcher::validateSolution(Solution solution) const
{
    in->setCheckpoint();
    try
    {
        // A map mapping column name to an index. This allows transforming multiple rows in one columns into one type.
        std::unordered_map<String, size_t> column_index;
        for (size_t i = 0; const auto & [name, _] : solution.columns)
        {
            column_index[name] = i++;
        }

        for (size_t row = 0; row < max_rows_to_check; ++row)
        {
            if (in->eof())
                break;


            size_t validated_columns = 0;
            for (const auto & i : solution.matchers_order)
            {
                skipWhitespacesAndDelimiters(*in);
                auto result = matchers[i]->parseField(*in, validated_columns);
                if (!result.ok)
                    break;

                auto type = result.name_and_type.type;
                auto name = result.name_and_type.name;
                if (!type || !column_index.contains(name))
                    break;

                auto type_index = column_index[name];
                matchers[i]->transformTypesIfPossible(solution.columns[type_index].type, type);
                if (!solution.columns[type_index].type->equals(*type))
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Received unexpected type after transform attempt");

                ++validated_columns;
            }

            if (validated_columns < solution.columns.size())
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unable to parse the desired number of fields");

            if (!in->eof() && *in->position() != '\n')
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Solution did not end at newline character");

            skipToNextLineOrEOF(*in);
        }

        in->rollbackToCheckpoint(true);
        return true;
    }
    catch (Exception & e)
    {
        LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "Solution fails: {}", e.message());
    }

    in->rollbackToCheckpoint(true);
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

    std::vector<Solution> solutions;
    buildSolutions(Solution{.score = 0}, solutions, false);
    if (solutions.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty solutions set");

    ::sort(
        solutions.begin(),
        solutions.end(),
        [](const Solution & first, const Solution & second)
        { return std::tie(first.score, first.matchers_order) > std::tie(second.score, second.matchers_order); });

    // after finding and ranking the solutions, we now run them through the next max_rows_to_check rows and pick the first one that works for all of them
    skipToNextLineOrEOF(*in);
    for (const auto & solution : solutions)
        if (validateSolution(solution))
        {
            final_solution = solution;
            return true;
        }

    return false;
}

bool FreeformFieldMatcher::parseRow()
{
    skipWhitespacesAndDelimiters(*in);
    if (in->eof() || final_solution.matchers_order.empty())
        return false;

    matched_fields.resize(final_solution.size);
    rules.resize(final_solution.size);

    for (size_t col = 0; const auto & i : final_solution.matchers_order)
    {
        skipWhitespacesAndDelimiters(*in);
        auto result = matchers[i]->parseField(*in, col);
        if (!first_row)
        {
            auto it = field_name_to_index.find(result.name_and_type.name);
            if (it != field_name_to_index.end())
                matched_fields[it->second] = result.field;
        }
        else
        {
            field_name_to_index[result.name_and_type.name] = col;
            rules[col] = matchers[i]->getEscapingRule();
            matched_fields[col] = result.field;
        }

        ++col;
    }

    first_row = false;
    skipToNextLineOrEOF(*in);
    return true;
}

FreeformRowInputFormat::FreeformRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), format_settings(format_settings_), matcher(in_, format_settings_)
{
}

bool FreeformRowInputFormat::readField(size_t index, MutableColumns & columns)
{
    const auto & type = matcher.getNamesAndTypes()[index].type;
    const auto rule = matcher.getRule(index);
    ReadBufferFromString field_buf(matcher.getField(index));

    return deserializeFieldByEscapingRule(type, serializations[index], *columns[index], field_buf, rule, format_settings);
}

bool FreeformRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
        return false;

    if (!matcher.buildSolutionsAndPickBest())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse freeform text, no solutions found");

    if (matcher.parseRow())
    {
        auto size = matcher.getSolutionLength();

        columns.resize(size);
        ext.read_columns.assign(size, false);
        for (size_t index = 0; index < size; ++index)
            ext.read_columns[index] = readField(index, columns);
    }

    return true;
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

DataTypes FreeformSchemaReader::readRowAndGetDataTypes()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "This method is not used and unimplemented for Freeform schema inference");
}

void registerInputFormatFreeform(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Freeform",
        [](ReadBuffer & buf, const Block & header, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<FreeformRowInputFormat>(buf, header, params, settings); });
}

void registerFreeformSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Freeform",
        [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<FreeformSchemaReader>(buf, settings); });
}
}
