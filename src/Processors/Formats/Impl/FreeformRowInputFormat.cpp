#include "Processors/Formats/Impl/FreeformRowInputFormat.h"
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include "Common/Exception.h"
#include "Common/StringUtils/StringUtils.h"
#include <Common/logger_useful.h>
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "Formats/EscapingRuleUtils.h"
#include "Formats/FormatFactory.h"
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

static inline void skipWhitespacesAndTabs(ReadBuffer & in)
{
    while (!in.eof() && (isWhitespaceASCII(*in.position()) || *in.position() == '-' || *in.position() == ':' || *in.position() == ','))
        ++in.position();
}

// Returns the score of the given type. This doesn't take nullable into account.
// Possible return values:
// - 1: String
// - 5: Decimal, Float
// - 10: Date, DateTime, Map, Array, Tuple
//
// TODO: Find a better way to define score criterions.
static size_t scoreForType(const DataTypePtr & type)
{
    WhichDataType which(type);

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return scoreForType(nullable_type->getNestedType());
    }

    if (which.isMap() || which.isArray() || which.isTuple())
        return 10;

    if (which.isDateOrDate32() || which.isDateTimeOrDateTime64())
        return 10;

    if (which.isDecimal() || which.isFloat())
        return 5;

    return 1;
}

// Returns if type is not String
static bool isPreferredType(const DataTypePtr & type)
{
    WhichDataType which(type);

    if (which.isNullable())
    {
        const auto * nullable_type = assert_cast<const DataTypeNullable *>(type.get());
        return isPreferredType(nullable_type->getNestedType());
    }

    return !which.isStringOrFixedString();
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

static size_t scoreForRule(const FormatSettings::EscapingRule & rule)
{
    switch (rule)
    {
        case FormatSettings::EscapingRule::JSON:
            return 10;
        case FormatSettings::EscapingRule::CSV:
            return 8;
        case FormatSettings::EscapingRule::Quoted:
            return 6;
        case FormatSettings::EscapingRule::Escaped:
            [[fallthrough]];
        case FormatSettings::EscapingRule::Raw:
            return 1;
        default:
            return 0;
    }
    UNREACHABLE();
}

static size_t scoreForField(const FormatSettings::EscapingRule & rule, DataTypePtr type)
{
    return scoreForRule(rule) * scoreForType(type);
}

void JSONFieldMatcher::parseField(String & s, ReadBuffer & in) const
{
    readJSONField(s, in);
}

void CSVFieldMatcher::parseField(String & s, ReadBuffer & in) const
{
    readCSVField(s, in, settings.csv);
}

void QuotedFieldMatcher::parseField(String & s, ReadBuffer & in) const
{
    readQuotedField(s, in);
}

void EscapedFieldMatcher::parseField(String & s, ReadBuffer & in) const
{
    readEscapedString(s, in);
}

void RawByWhitespaceFieldMatcher::parseField(String & s, ReadBuffer & in) const
{
    readStringUntilWhitespaceDelimiter(s, in);
}

FreeformFieldMatcher::FreeformFieldMatcher(ReadBuffer & in_, const FormatSettings & settings_)
    : format_settings(settings_), max_rows_to_check(std::min<size_t>(100, settings_.max_rows_to_read_for_schema_inference)), in(in_)
{
    // matchers are pushed in the order of priority, this helps with exiting early and reducing the search tree.
    matchers.push_back(std::make_unique<JSONFieldMatcher>(FormatSettings::EscapingRule::JSON, settings_));
    matchers.push_back(std::make_unique<CSVFieldMatcher>(FormatSettings::EscapingRule::CSV, settings_));
    matchers.push_back(std::make_unique<RawByWhitespaceFieldMatcher>(FormatSettings::EscapingRule::Raw, settings_));
    matchers.push_back(std::make_unique<QuotedFieldMatcher>(FormatSettings::EscapingRule::Quoted, settings_));
    matchers.push_back(std::make_unique<EscapedFieldMatcher>(FormatSettings::EscapingRule::Escaped, settings_));
}

std::vector<FreeformFieldMatcher::Field> FreeformFieldMatcher::readNextPossibleFields(const bool & one_string)
{
    skipWhitespacesAndTabs(in);
    char * start = in.position();
    std::vector<FreeformFieldMatcher::Field> fields;

    String field;
    if (one_string)
    {
        matchers.back()->parseField(field, in);
        auto type = matchers.back()->getDataTypeFromField(field);
        fields.emplace_back(type, matchers.size() - 1, scoreForField(FormatSettings::EscapingRule::JSON, type), in.position(), false);
        return fields;
    }

    for (size_t i = 0; const auto & matcher : matchers)
    {
        try
        {
            matcher->parseField(field, in);
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "got field: {} ; matcher: {}", field, matcher->getName());
            auto type = matcher->getDataTypeFromField(field);
            if (type)
            {
                type = makeFloatIfInt(type); // for the sake of correctness, make all int to be float
                fields.emplace_back(
                    type,
                    i,
                    scoreForField(matcher->getEscapingRule(), type),
                    in.position(),
                    field.ends_with(':') && matcher->getName() == "RawByWhitespaceFieldMatcher");
                LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "got type: {}", type->getName());
                if (isPreferredType(type))
                    break;
            }
        }
        catch (Exception & e)
        {
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "failed to parse field, error: {}", e.message());
        }

        ++i;
        in.position() = start;
    }

    return fields;
}

void FreeformFieldMatcher::recursivelyGetNextFieldInRow(
    char * current_pos, Solution current_solution, std::vector<Solution> & solutions, const bool & one_string)
{
    char * tmp = in.position();
    in.position() = current_pos;
    if (*in.position() == '\n')
    {
        solutions.push_back(current_solution);
        LOG_DEBUG(
            &Poco::Logger::get("FreeformFieldMatcher"),
            "got new solution with score: {}, total solutions: {}",
            current_solution.score,
            solutions.size());
        // not resetting the buffer as we've already reach end of row
        return;
    }

    const auto fields = readNextPossibleFields(one_string);
    for (const auto & field : fields)
    {
        auto next = current_solution;
        next.matchers_order.push_back(field.matcher_index);
        next.matched_types.push_back(field.type);
        next.score += field.score;

        recursivelyGetNextFieldInRow(field.pos, next, solutions, field.should_parse_the_rest_as_one_string);
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
    skipBOMIfExists(in);
    if (in.eof())
        return false;

    char * start = in.position();

    std::vector<Solution> solutions;
    readRowAndGenerateSolutions(in.position(), solutions);
    if (solutions.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty solution.");

    ::sort(solutions.begin(), solutions.end(), [](const Solution & first, const Solution & second) { return first.score > second.score; });
    in.position() = start;

    // after finding and ranking the solutions, we now run them through max_rows_to_check and pick the first one that works for all rows
    String tmp;
    for (const auto & solution : solutions)
    {
        try
        {
            for (size_t row = 0; row < max_rows_to_check; ++row)
            {
                if (in.eof())
                    break;

                for (const auto & index : solution.matchers_order)
                {
                    skipWhitespacesAndTabs(in);
                    matchers[index]->parseField(tmp, in);
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
            LOG_DEBUG(&Poco::Logger::get("FreeformFieldMatcher"), "failed to parse data, error: {} ", e.message());
        }
    }

    in.position() = start;
    return false;
}

bool FreeformFieldMatcher::parseRow()
{
    skipWhitespacesAndTabs(in);
    if (in.eof() || final_solution.matchers_order.empty())
        return false;

    matched_fields.resize(final_solution.matchers_order.size());
    for (size_t col = 0; const auto & i : final_solution.matchers_order)
    {
        skipWhitespacesAndTabs(in);
        matchers[i]->parseField(matched_fields[col], in);
        ++col;
    }

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
    const auto & type = matcher.getDataTypes()[index];
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

FreeformSchemaReader::FreeformSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IRowSchemaReader(in_, format_settings_), matcher(in_, format_settings_)
{
}

NamesAndTypesList FreeformSchemaReader::readSchema()
{
    if (!matcher.generateSolutionsAndPickBest())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unable to parse freeform text, no solutions found.");

    DataTypes types = matcher.getDataTypes();
    NamesAndTypesList names_and_types;

    for (size_t i = 0; i < types.size(); ++i)
        names_and_types.emplace_back(fmt::format("c{}", i), types[i]);

    return names_and_types;
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
