#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONEachRowUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/Parser.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/ExpressionListParsers.h>
#include <Interpreters/evaluateConstantExpression.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

FormatSettings::EscapingRule stringToEscapingRule(const String & escaping_rule)
{
    if (escaping_rule.empty())
        return FormatSettings::EscapingRule::None;
    else if (escaping_rule == "None")
        return FormatSettings::EscapingRule::None;
    else if (escaping_rule == "Escaped")
        return FormatSettings::EscapingRule::Escaped;
    else if (escaping_rule == "Quoted")
        return FormatSettings::EscapingRule::Quoted;
    else if (escaping_rule == "CSV")
        return FormatSettings::EscapingRule::CSV;
    else if (escaping_rule == "JSON")
        return FormatSettings::EscapingRule::JSON;
    else if (escaping_rule == "XML")
        return FormatSettings::EscapingRule::XML;
    else if (escaping_rule == "Raw")
        return FormatSettings::EscapingRule::Raw;
    else
        throw Exception("Unknown escaping rule \"" + escaping_rule + "\"", ErrorCodes::BAD_ARGUMENTS);
}

String escapingRuleToString(FormatSettings::EscapingRule escaping_rule)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::None:
            return "None";
        case FormatSettings::EscapingRule::Escaped:
            return "Escaped";
        case FormatSettings::EscapingRule::Quoted:
            return "Quoted";
        case FormatSettings::EscapingRule::CSV:
            return "CSV";
        case FormatSettings::EscapingRule::JSON:
            return "JSON";
        case FormatSettings::EscapingRule::XML:
            return "XML";
        case FormatSettings::EscapingRule::Raw:
            return "Raw";
    }
    __builtin_unreachable();
}

void skipFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    String tmp;
    constexpr const char * field_name = "<SKIPPED COLUMN>";
    constexpr size_t field_name_len = 16;
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::None:
            /// Empty field, just skip spaces
            break;
        case FormatSettings::EscapingRule::Escaped:
            readEscapedString(tmp, buf);
            break;
        case FormatSettings::EscapingRule::Quoted:
            readQuotedFieldIntoString(tmp, buf);
            break;
        case FormatSettings::EscapingRule::CSV:
            readCSVString(tmp, buf, format_settings.csv);
            break;
        case FormatSettings::EscapingRule::JSON:
            skipJSONField(buf, StringRef(field_name, field_name_len));
            break;
        case FormatSettings::EscapingRule::Raw:
            readString(tmp, buf);
            break;
        default:
            __builtin_unreachable();
    }
}

bool deserializeFieldByEscapingRule(
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    IColumn & column,
    ReadBuffer & buf,
    FormatSettings::EscapingRule escaping_rule,
    const FormatSettings & format_settings)
{
    bool read = true;
    bool parse_as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Escaped:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeTextEscapedImpl(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextEscaped(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::Quoted:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeTextQuotedImpl(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextQuoted(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::CSV:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeTextCSVImpl(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextCSV(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::JSON:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeTextJSONImpl(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextJSON(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::Raw:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeTextRawImpl(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextRaw(column, buf, format_settings);
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Escaping rule {} is not suitable for deserialization", escapingRuleToString(escaping_rule));
    }
    return read;
}

void serializeFieldByEscapingRule(
    const IColumn & column,
    const ISerialization & serialization,
    WriteBuffer & out,
    size_t row_num,
    FormatSettings::EscapingRule escaping_rule,
    const FormatSettings & format_settings)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Escaped:
            serialization.serializeTextEscaped(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::Quoted:
            serialization.serializeTextQuoted(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::CSV:
            serialization.serializeTextCSV(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::JSON:
            serialization.serializeTextJSON(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::XML:
            serialization.serializeTextXML(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::Raw:
            serialization.serializeTextRaw(column, row_num, out, format_settings);
            break;
        case FormatSettings::EscapingRule::None:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot serialize field with None escaping rule");
    }
}

void writeStringByEscapingRule(const String & value, WriteBuffer & out, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
            writeQuotedString(value, out);
            break;
        case FormatSettings::EscapingRule::JSON:
            writeJSONString(value, out, format_settings);
            break;
        case FormatSettings::EscapingRule::Raw:
            writeString(value, out);
            break;
        case FormatSettings::EscapingRule::CSV:
            writeCSVString(value, out);
            break;
        case FormatSettings::EscapingRule::Escaped:
            writeEscapedString(value, out);
            break;
        case FormatSettings::EscapingRule::XML:
            writeXMLStringForTextElement(value, out);
            break;
        case FormatSettings::EscapingRule::None:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot serialize string with None escaping rule");
    }
}

template <bool read_string>
String readByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    String result;
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
            if constexpr (read_string)
                readQuotedString(result, buf);
            else
                readQuotedFieldIntoString(result, buf);
            break;
        case FormatSettings::EscapingRule::JSON:
            if constexpr (read_string)
                readJSONString(result, buf);
            else
                readJSONFieldIntoString(result, buf);
            break;
        case FormatSettings::EscapingRule::Raw:
            readString(result, buf);
            break;
        case FormatSettings::EscapingRule::CSV:
            if constexpr (read_string)
                readCSVString(result, buf, format_settings.csv);
            else
                readCSVField(result, buf, format_settings.csv);
            break;
        case FormatSettings::EscapingRule::Escaped:
            readEscapedString(result, buf);
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read value with {} escaping rule", escapingRuleToString(escaping_rule));
    }
    return result;
}

String readFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    return readByEscapingRule<false>(buf, escaping_rule, format_settings);
}

String readStringByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    return readByEscapingRule<true>(buf, escaping_rule, format_settings);
}

static bool evaluateConstantExpressionFromString(const StringRef & field, DataTypePtr & type, ContextPtr context)
{
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "You must provide context to evaluate constant expression");

    ParserExpression parser;
    Expected expected;
    Tokens tokens(field.data, field.data + field.size);
    IParser::Pos token_iterator(tokens, context->getSettingsRef().max_parser_depth);
    ASTPtr ast;

    /// FIXME: Our parser cannot parse maps in the form of '{key : value}' that is used in text formats.
    bool parsed = parser.parse(token_iterator, ast, expected);
    if (!parsed || !token_iterator->isEnd())
        return false;

    try
    {
        std::pair<Field, DataTypePtr> result = evaluateConstantExpression(ast, context);
        type = generalizeDataType(result.second);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

DataTypePtr determineDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, ContextPtr context)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
        {
            DataTypePtr type;
            bool parsed = evaluateConstantExpressionFromString(field, type, context);
            return parsed ? type : nullptr;
        }
        case FormatSettings::EscapingRule::JSON:
            return getDataTypeFromJSONField(field);
        case FormatSettings::EscapingRule::CSV:
        {
            if (field.empty() || field == format_settings.csv.null_representation)
                return nullptr;

            if (field == format_settings.bool_false_representation || field == format_settings.bool_true_representation)
                return std::make_shared<DataTypeUInt8>();

            DataTypePtr type;
            bool parsed;
            if (field[0] == '\'' || field[0] == '"')
            {
                /// Try to evaluate expression inside quotes.
                parsed = evaluateConstantExpressionFromString(StringRef(field.data() + 1, field.size() - 2), type, context);
                /// If it's a number in quotes we determine it as a string.
                if (parsed && type && isNumber(removeNullable(type)))
                    return makeNullable(std::make_shared<DataTypeString>());
            }
            else
                parsed = evaluateConstantExpressionFromString(field, type, context);

            /// If we couldn't parse an expression, determine it as a string.
            return parsed ? type : makeNullable(std::make_shared<DataTypeString>());
        }
        case FormatSettings::EscapingRule::Raw: [[fallthrough]];
        case FormatSettings::EscapingRule::Escaped:
            /// TODO: Try to use some heuristics here to determine the type of data.
            return field.empty() ? nullptr : makeNullable(std::make_shared<DataTypeString>());
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the type for value with {} escaping rule", escapingRuleToString(escaping_rule));
    }
}

DataTypes determineDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, ContextPtr context)
{
    DataTypes data_types;
    data_types.reserve(fields.size());
    for (const auto & field : fields)
        data_types.push_back(determineDataTypeByEscapingRule(field, format_settings, escaping_rule, context));
    return data_types;
}

DataTypePtr getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule escaping_rule)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::CSV: [[fallthrough]];
        case FormatSettings::EscapingRule::Escaped: [[fallthrough]];
        case FormatSettings::EscapingRule::Raw:
            return makeNullable(std::make_shared<DataTypeString>());
        default:
            return nullptr;
    }
}

}
