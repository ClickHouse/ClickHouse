#include <Formats/EscapingRuleUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

String readStringByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    String result;
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
            readQuotedString(result, buf);
            break;
        case FormatSettings::EscapingRule::JSON:
            readJSONString(result, buf);
            break;
        case FormatSettings::EscapingRule::Raw:
            readString(result, buf);
            break;
        case FormatSettings::EscapingRule::CSV:
            readCSVString(result, buf, format_settings.csv);
            break;
        case FormatSettings::EscapingRule::Escaped:
            readEscapedString(result, buf);
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read string with {} escaping rule", escapingRuleToString(escaping_rule));
    }
    return result;
}

}
