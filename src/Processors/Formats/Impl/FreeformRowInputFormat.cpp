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
// Possible return values:
// - 1: String
// - 5: Decimal, Float
// - 10: Date, DateTime, Map, Array, Tuple
static size_t scoreForType(const DataTypePtr & type)
{
    WhichDataType which(type);

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return scoreForType(nullable_type->getNestedType());
    }

    if (which.isMap() || which.isArray() || which.isTuple())
        return 100;

    if (which.isDateOrDate32() || which.isDateTimeOrDateTime64())
        // the rationale here is 2022.11.28 11:30:04.474043 could be parsed into as many as 7 fields
        // so the score for dates must be at least 7 times greater than a number
        return 50;

    if (which.isDecimal() || which.isFloat())
        return 5;

    return 1;
}

static size_t scoreForRule(FormatSettings::EscapingRule rule)
{
    switch (rule)
    {
        case FormatSettings::EscapingRule::JSON:
            return 5;
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

static DataTypePtr makeFloatIfInt(const DataTypePtr & type)
{
    WhichDataType which(type);

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return makeNullable(makeFloatIfInt(nullable_type->getNestedType()));
    }

    if (which.isInt() || which.isUInt())
    {
        return std::make_shared<DataTypeFloat64>();
    }

    return type;
}


std::vector<std::pair<String, String>> JSONFieldMatcher::parseFields(ReadBuffer & in, size_t index) const
{
    if (in.eof())
        return {};

    if (*in.position() != '{')
    {
        String ret;
        readJSONField(ret, in);
        return {{fmt::format("c{}", index), ret}};
    }


    ++in.position();
    skipWhitespacesAndDelimiters(in);

    std::vector<std::pair<String, String>> ret;
    while (*in.position() != '}')
    {
        String field_name = JSONUtils::readFieldName(in);

        auto fields = parseFields(in, index); // "index" here doesn't do anything

        for (const auto & [name, field] : fields)
        {
            if (name.starts_with("c"))
                ret.emplace_back(field_name, field);
            else
                ret.emplace_back(fmt::format("{}.{}", field_name, name), field);
        }

        skipWhitespacesAndDelimiters(in);
    }

    ++in.position();

    return ret;
}

std::vector<std::pair<String, String>> CSVFieldMatcher::parseFields(ReadBuffer & in, size_t index) const
{
    String ret;
    readCSVField(ret, in, settings.csv);
    return {{fmt::format("c{}", index), ret}};
}

std::vector<std::pair<String, String>> QuotedFieldMatcher::parseFields(ReadBuffer & in, size_t index) const
{
    String ret;
    readQuotedField(ret, in);
    return {{fmt::format("c{}", index), ret}};
}

std::vector<std::pair<String, String>> EscapedFieldMatcher::parseFields(ReadBuffer & in, size_t index) const
{
    String ret;
    readEscapedString(ret, in);
    return {{fmt::format("c{}", index), ret}};
}

std::vector<std::pair<String, String>> RawByWhitespaceFieldMatcher::parseFields(ReadBuffer & in, size_t index) const
{
    String ret;
    readStringUntilWhitespaceDelimiter(ret, in);
    return {{fmt::format("c{}", index), ret}};
}

FreeformFieldMatcher::FreeformFieldMatcher(ReadBuffer & in_, const FormatSettings & settings_)
    : format_settings(settings_), max_rows_to_check(std::min<size_t>(100, settings_.max_rows_to_read_for_schema_inference)), in(in_)
{
    // matchers are pushed in the order of priority, this helps with exiting early and reducing the search tree.
    matchers.emplace_back(std::make_unique<JSONFieldMatcher>(FormatSettings::EscapingRule::JSON, settings_));
    matchers.emplace_back(std::make_unique<CSVFieldMatcher>(FormatSettings::EscapingRule::CSV, settings_));
    matchers.emplace_back(std::make_unique<RawByWhitespaceFieldMatcher>(FormatSettings::EscapingRule::Raw, settings_));
    matchers.emplace_back(std::make_unique<QuotedFieldMatcher>(FormatSettings::EscapingRule::Quoted, settings_));
    matchers.emplace_back(std::make_unique<EscapedFieldMatcher>(FormatSettings::EscapingRule::Escaped, settings_));
}

std::vector<FreeformFieldMatcher::Fields> FreeformFieldMatcher::readNextFields(bool one_string, size_t index)
{
    skipWhitespacesAndDelimiters(in);
    char * start = in.position();
    std::vector<FreeformFieldMatcher::Fields> next_fields;

    if (one_string)
    {
        auto fields = matchers.back()->parseFields(in, index);
        NamesAndTypes columns{{fields[0].first, matchers.back()->getDataTypeFromField(fields[0].second)}};
        next_fields.emplace_back(
            columns, matchers.size() - 1, scoreForField(matchers.back()->getEscapingRule(), columns[0].type), in.position(), false);
        return next_fields;
    }

    size_t best_type_score = 0, type_score = 0, fields_score = 0;
    for (size_t i = 0; const auto & matcher : matchers)
    {
        try
        {
            auto fields = matcher->parseFields(in, index);
            NamesAndTypes columns; // we either add all of the fields or none of them
            type_score = 0;
            fields_score = 0;

            for (const auto & [name, field] : fields)
            {
                auto type = matcher->getDataTypeFromField(field);
                if (type)
                {
                    // a single punctuation is not a valid field
                    if (field.size() == 1 && isPunctuationASCII(field[0]))
                        continue;

                    type = makeFloatIfInt(type);
                    columns.emplace_back(name, type);

                    type_score += scoreForType(type);
                    fields_score += scoreForField(matcher->getEscapingRule(), type);

                    one_string = field.ends_with(':') && matcher->getName() == "RawByWhitespaceFieldMatcher";
                }
            }

            // add a new field only if type_score > best_type_score or if we haven't found a field other than string
            if (columns.size() == fields.size() && (type_score > best_type_score || best_type_score <= 1))
            {
                best_type_score = type_score;
                next_fields.emplace_back(columns, i, fields_score, in.position(), one_string);
            }
        }
        catch (Exception & e)
        {
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "fail to parse field: {}", e.message());
        }

        ++i;
        in.position() = start;
    }

    return next_fields;
}

void FreeformFieldMatcher::recursivelyGetNextFieldInRow(
    char * current_pos, Solution current_solution, std::vector<Solution> & solutions, bool one_string)
{
    char * tmp = in.position();
    in.position() = current_pos;
    if (*in.position() == '\n')
    {
        solutions.push_back(current_solution);
        return;
    }

    const auto next_fields = readNextFields(one_string, current_solution.size);
    for (const auto & fields : next_fields)
    {
        auto next = current_solution;
        next.matchers_order.push_back(fields.matcher_index);
        for (const auto & column : fields.columns)
            next.columns.push_back(column);

        next.score += fields.score;
        next.size += fields.columns.size();

        recursivelyGetNextFieldInRow(fields.pos, next, solutions, fields.parse_the_rest_as_one_string);
    }

    in.position() = tmp; // reset to initial position
}

void FreeformFieldMatcher::readRowAndGenerateSolutions(char * pos, std::vector<Solution> & solutions)
{
    Solution current_solution{.score = 0};
    recursivelyGetNextFieldInRow(pos, current_solution, solutions, false);
    skipToNextLineOrEOF(in);
}

bool FreeformFieldMatcher::generateSolutionsAndPickBest()
{
    if (!final_solution.matchers_order.empty())
        // if a solution is found already, we could return immediately
        // this is useful in the case of readRow
        //
        // temporary hack until we could reuse the solution generated in readSchema
        return true;

    skipBOMIfExists(in);
    if (in.eof())
        return false;

    char * start = in.position();

    std::vector<Solution> solutions;
    readRowAndGenerateSolutions(in.position(), solutions);
    if (solutions.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty solution.");

    ::sort(
        solutions.begin(),
        solutions.end(),
        [](const Solution & first, const Solution & second)
        {
            if (first.score != second.score)
                return first.score > second.score;

            return first.matchers_order.size() > second.matchers_order.size();
        });

    // after finding and ranking the solutions, we now run them through max_rows_to_check and pick the first one that works for all rows
    for (const auto & solution : solutions)
    {
        try
        {
            in.position() = start;
            for (size_t row = 0; row < max_rows_to_check; ++row)
            {
                if (in.eof())
                    break;

                for (const auto & i : solution.matchers_order)
                {
                    skipWhitespacesAndDelimiters(in);
                    matchers[i]->parseFields(in, 0);
                }

                if (!in.eof() && *in.position() != '\n')
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Solution did not end at newline character.");

                skipToNextLineOrEOF(in);
            }

            final_solution = solution;
            in.position() = start;
            return true;
        }
        catch (Exception & e)
        {
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "fail to parse field: {}", e.message());
        }
    }

    in.position() = start;
    return false;
}

bool FreeformFieldMatcher::parseRow()
{
    skipWhitespacesAndDelimiters(in);
    if (in.eof() || final_solution.matchers_order.empty())
        return false;

    matched_fields.resize(final_solution.size);
    rules.resize(final_solution.size);

    for (size_t col = 0; const auto & i : final_solution.matchers_order)
    {
        skipWhitespacesAndDelimiters(in);
        auto fields = matchers[i]->parseFields(in, col);
        for (const auto & [name, field] : fields)
        {
            if (!first_row)
                matched_fields[field_name_to_index[name]] = field;
            else
            {
                field_name_to_index[name] = col;
                rules[col] = matchers[i]->getEscapingRule();
                matched_fields[col] = field;
            }

            ++col;
        }
    }

    first_row = false;
    skipToNextLineOrEOF(in);
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

    if (!matcher.generateSolutionsAndPickBest())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse freeform text, no solutions found.");

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
    if (!matcher.generateSolutionsAndPickBest())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse freeform text, no solutions found.");

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
