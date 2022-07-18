#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONEachRowUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Parsers/TokenIterator.h>


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
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Escaping rule {} is not suitable for deserialization", escapingRuleToString(escaping_rule));
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

void writeStringByEscapingRule(
    const String & value, WriteBuffer & out, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
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

static DataTypePtr determineDataTypeForSingleFieldImpl(ReadBuffer & buf)
{
    if (buf.eof())
        return nullptr;

    /// Array
    if (checkChar('[', buf))
    {
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        while (!buf.eof() && *buf.position() != ']')
        {
            if (!first)
            {
                skipWhitespaceIfAny(buf);
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = determineDataTypeForSingleFieldImpl(buf);
            if (!nested_type)
                return nullptr;

            nested_types.push_back(nested_type);
        }

        if (buf.eof())
            return nullptr;

        ++buf.position();

        if (nested_types.empty())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());

        auto least_supertype = tryGetLeastSupertype(nested_types);
        if (!least_supertype)
            return nullptr;

        return std::make_shared<DataTypeArray>(least_supertype);
    }

    /// Tuple
    if (checkChar('(', buf))
    {
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        while (!buf.eof() && *buf.position() != ')')
        {
            if (!first)
            {
                skipWhitespaceIfAny(buf);
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = determineDataTypeForSingleFieldImpl(buf);
            if (!nested_type)
                return nullptr;

            nested_types.push_back(nested_type);
        }

        if (buf.eof() || nested_types.empty())
            return nullptr;

        ++buf.position();

        return std::make_shared<DataTypeTuple>(nested_types);
    }

    /// Map
    if (checkChar('{', buf))
    {
        skipWhitespaceIfAny(buf);

        DataTypes key_types;
        DataTypes value_types;
        bool first = true;
        while (!buf.eof() && *buf.position() != '}')
        {
            if (!first)
            {
                skipWhitespaceIfAny(buf);
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto key_type = determineDataTypeForSingleFieldImpl(buf);
            if (!key_type)
                return nullptr;

            key_types.push_back(key_type);

            skipWhitespaceIfAny(buf);
            if (!checkChar(':', buf))
                return nullptr;
            skipWhitespaceIfAny(buf);

            auto value_type = determineDataTypeForSingleFieldImpl(buf);
            if (!value_type)
                return nullptr;

            value_types.push_back(value_type);
        }

        if (buf.eof())
            return nullptr;

        ++buf.position();
        skipWhitespaceIfAny(buf);

        if (key_types.empty())
            return std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());

        auto key_least_supertype = tryGetLeastSupertype(key_types);

        auto value_least_supertype = tryGetLeastSupertype(value_types);
        if (!key_least_supertype || !value_least_supertype)
            return nullptr;

        if (!DataTypeMap::checkKeyType(key_least_supertype))
            return nullptr;

        return std::make_shared<DataTypeMap>(key_least_supertype, value_least_supertype);
    }

    /// String
    if (*buf.position() == '\'')
    {
        ++buf.position();
        while (!buf.eof())
        {
            char * next_pos = find_first_symbols<'\\', '\''>(buf.position(), buf.buffer().end());
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            if (*buf.position() == '\'')
                break;

            if (*buf.position() == '\\')
                ++buf.position();
        }

        if (buf.eof())
            return nullptr;

        ++buf.position();
        return std::make_shared<DataTypeString>();
    }

    /// Bool
    if (checkStringCaseInsensitive("true", buf) || checkStringCaseInsensitive("false", buf))
        return DataTypeFactory::instance().get("Bool");

    /// Null
    if (checkStringCaseInsensitive("NULL", buf))
        return std::make_shared<DataTypeNothing>();

    /// Number
    Float64 tmp;
    if (tryReadFloatText(tmp, buf))
        return std::make_shared<DataTypeFloat64>();

    return nullptr;
}

static DataTypePtr determineDataTypeForSingleField(ReadBuffer & buf)
{
    return makeNullableRecursivelyAndCheckForNothing(determineDataTypeForSingleFieldImpl(buf));
}

DataTypePtr determineDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
        {
            ReadBufferFromString buf(field);
            auto type = determineDataTypeForSingleField(buf);
            return buf.eof() ? type : nullptr;
        }
        case FormatSettings::EscapingRule::JSON:
            return getDataTypeFromJSONField(field);
        case FormatSettings::EscapingRule::CSV:
        {
            if (!format_settings.csv.input_format_use_best_effort_in_schema_inference)
                return makeNullable(std::make_shared<DataTypeString>());

            if (field.empty() || field == format_settings.csv.null_representation)
                return nullptr;

            if (field == format_settings.bool_false_representation || field == format_settings.bool_true_representation)
                return DataTypeFactory::instance().get("Nullable(Bool)");

            if (field.size() > 1 && ((field.front() == '\'' && field.back() == '\'') || (field.front() == '"' && field.back() == '"')))
            {
                ReadBufferFromString buf(std::string_view(field.data() + 1, field.size() - 2));
                /// Try to determine the type of value inside quotes
                auto type = determineDataTypeForSingleField(buf);

                if (!type)
                    return nullptr;

                /// If it's a number or tuple in quotes or there is some unread data in buffer, we determine it as a string.
                if (isNumber(removeNullable(type)) || isTuple(type) || !buf.eof())
                    return makeNullable(std::make_shared<DataTypeString>());

                return type;
            }

            /// Case when CSV value is not in quotes. Check if it's a number, and if not, determine it's as a string.
            ReadBufferFromString buf(field);
            Float64 tmp;
            if (tryReadFloatText(tmp, buf) && buf.eof())
                return makeNullable(std::make_shared<DataTypeFloat64>());

            return makeNullable(std::make_shared<DataTypeString>());
        }
        case FormatSettings::EscapingRule::Raw: [[fallthrough]];
        case FormatSettings::EscapingRule::Escaped:
        {
            if (!format_settings.tsv.input_format_use_best_effort_in_schema_inference)
                return makeNullable(std::make_shared<DataTypeString>());

            if (field.empty() || field == format_settings.tsv.null_representation)
                return nullptr;

            if (field == format_settings.bool_false_representation || field == format_settings.bool_true_representation)
                return DataTypeFactory::instance().get("Nullable(Bool)");

            ReadBufferFromString buf(field);
            auto type = determineDataTypeForSingleField(buf);
            if (!buf.eof())
                return makeNullable(std::make_shared<DataTypeString>());

            return type;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the type for value with {} escaping rule", escapingRuleToString(escaping_rule));
    }
}

DataTypes determineDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule)
{
    DataTypes data_types;
    data_types.reserve(fields.size());
    for (const auto & field : fields)
        data_types.push_back(determineDataTypeByEscapingRule(field, format_settings, escaping_rule));
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

DataTypes getDefaultDataTypeForEscapingRules(const std::vector<FormatSettings::EscapingRule> & escaping_rules)
{
    DataTypes data_types;
    for (const auto & rule : escaping_rules)
        data_types.push_back(getDefaultDataTypeForEscapingRule(rule));
    return data_types;
}

}
