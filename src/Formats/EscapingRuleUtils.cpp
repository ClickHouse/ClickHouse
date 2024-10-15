#include <Formats/EscapingRuleUtils.h>
#include <Formats/SchemaInferenceUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
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
    if (escaping_rule == "None")
        return FormatSettings::EscapingRule::None;
    if (escaping_rule == "Escaped")
        return FormatSettings::EscapingRule::Escaped;
    if (escaping_rule == "Quoted")
        return FormatSettings::EscapingRule::Quoted;
    if (escaping_rule == "CSV")
        return FormatSettings::EscapingRule::CSV;
    if (escaping_rule == "JSON")
        return FormatSettings::EscapingRule::JSON;
    if (escaping_rule == "XML")
        return FormatSettings::EscapingRule::XML;
    if (escaping_rule == "Raw")
        return FormatSettings::EscapingRule::Raw;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown escaping rule \"{}\"", escaping_rule);
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
}

void skipFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    NullOutput out;
    constexpr const char * field_name = "<SKIPPED COLUMN>";
    constexpr size_t field_name_len = 16;
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::None:
            /// Empty field, just skip spaces
            break;
        case FormatSettings::EscapingRule::Escaped:
            readEscapedStringInto<NullOutput,false>(out, buf);
            break;
        case FormatSettings::EscapingRule::Quoted:
            readQuotedFieldInto(out, buf);
            break;
        case FormatSettings::EscapingRule::CSV:
            readCSVStringInto(out, buf, format_settings.csv);
            break;
        case FormatSettings::EscapingRule::JSON:
            skipJSONField(buf, StringRef(field_name, field_name_len), format_settings.json);
            break;
        case FormatSettings::EscapingRule::Raw:
            readStringInto(out, buf);
            break;
        default:
            UNREACHABLE();
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
    bool parse_as_nullable = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type);
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Escaped:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeNullAsDefaultOrNestedTextEscaped(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextEscaped(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::Quoted:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextQuoted(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::CSV:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextCSV(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::JSON:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(column, buf, format_settings, serialization);
            else
                serialization->deserializeTextJSON(column, buf, format_settings);
            break;
        case FormatSettings::EscapingRule::Raw:
            if (parse_as_nullable)
                read = SerializationNullable::deserializeNullAsDefaultOrNestedTextRaw(column, buf, format_settings, serialization);
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
                readQuotedField(result, buf);
            break;
        case FormatSettings::EscapingRule::JSON:
            if constexpr (read_string)
                readJSONString(result, buf, format_settings.json);
            else
                readJSONField(result, buf, format_settings.json);
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
            if constexpr (read_string)
                readEscapedString(result, buf);
            else
                readTSVField(result, buf);
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

String readStringOrFieldByEscapingRule(ReadBuffer & buf, FormatSettings::EscapingRule escaping_rule, const FormatSettings & format_settings)
{
    /// For Quoted escaping rule we can read value as string only if it starts with `'`.
    /// If there is no `'` it can be any other field number/array/etc.
    if (escaping_rule == FormatSettings::EscapingRule::Quoted && !buf.eof() && *buf.position() != '\'')
        return readFieldByEscapingRule(buf, escaping_rule, format_settings);

    /// For JSON it's the same as for Quoted, but we check `"`.
    if (escaping_rule == FormatSettings::EscapingRule::JSON && !buf.eof() && *buf.position() != '"')
        return readFieldByEscapingRule(buf, escaping_rule, format_settings);

    /// For other escaping rules we can read any field as string value.
    return readStringByEscapingRule(buf, escaping_rule, format_settings);
}

DataTypePtr tryInferDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
            return tryInferDataTypeForSingleField(field, format_settings);
        case FormatSettings::EscapingRule::JSON:
            return tryInferDataTypeForSingleJSONField(field, format_settings, json_info);
        case FormatSettings::EscapingRule::CSV:
        {
            if (!format_settings.csv.use_best_effort_in_schema_inference)
                return std::make_shared<DataTypeString>();

            if (field.empty())
                return nullptr;

            if (field == format_settings.csv.null_representation)
                return makeNullable(std::make_shared<DataTypeNothing>());

            if (field == format_settings.bool_false_representation || field == format_settings.bool_true_representation)
                return DataTypeFactory::instance().get("Bool");

            /// In CSV complex types are serialized in quotes. If we have quotes, we should try to infer type
            /// from data inside quotes.
            if (field.size() > 1 && ((field.front() == '\'' && field.back() == '\'') || (field.front() == '"' && field.back() == '"')))
            {
                auto data = std::string_view(field.data() + 1, field.size() - 2);
                /// First, try to infer dates and datetimes.
                if (auto date_type = tryInferDateOrDateTimeFromString(data, format_settings))
                    return date_type;

                /// Try to determine the type of value inside quotes
                auto type = tryInferDataTypeForSingleField(data, format_settings);

                /// Return String type if one of the following conditions apply
                ///  - we couldn't infer any type
                ///  - it's a number and csv.try_infer_numbers_from_strings = 0
                ///  - it's a tuple and try_infer_strings_from_quoted_tuples = 0
                ///  - it's a Bool type (we don't allow reading bool values from strings)
                if (!type || (format_settings.csv.try_infer_strings_from_quoted_tuples && isTuple(type)) || (!format_settings.csv.try_infer_numbers_from_strings && isNumber(type)) || isBool(type))
                    return std::make_shared<DataTypeString>();

                return type;
            }

            /// Case when CSV value is not in quotes. Check if it's a number or date/datetime, and if not, determine it as a string.
            if (auto number_type = tryInferNumberFromString(field, format_settings))
                return number_type;

            if (auto date_type = tryInferDateOrDateTimeFromString(field, format_settings))
                return date_type;

            return std::make_shared<DataTypeString>();
        }
        case FormatSettings::EscapingRule::Raw: [[fallthrough]];
        case FormatSettings::EscapingRule::Escaped:
        {
            if (!format_settings.tsv.use_best_effort_in_schema_inference)
                return std::make_shared<DataTypeString>();

            if (field.empty())
                return nullptr;

            if (field == format_settings.tsv.null_representation)
                return makeNullable(std::make_shared<DataTypeNothing>());

            if (field == format_settings.bool_false_representation || field == format_settings.bool_true_representation)
                return DataTypeFactory::instance().get("Bool");

            if (auto date_type = tryInferDateOrDateTimeFromString(field, format_settings))
                return date_type;

            /// Special case when we have number that starts with 0. In TSV we don't parse such numbers,
            /// see readIntTextUnsafe in ReadHelpers.h. If we see data started with 0, we can determine it
            /// as a String, so parsing won't fail.
            if (field[0] == '0' && field.size() != 1)
                return std::make_shared<DataTypeString>();

            auto type = tryInferDataTypeForSingleField(field, format_settings);
            if (!type)
                return std::make_shared<DataTypeString>();
            return type;
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine the type for value with {} escaping rule",
                            escapingRuleToString(escaping_rule));
    }
}

DataTypes tryInferDataTypesByEscapingRule(const std::vector<String> & fields, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info)
{
    DataTypes data_types;
    data_types.reserve(fields.size());
    for (const auto & field : fields)
        data_types.push_back(tryInferDataTypeByEscapingRule(field, format_settings, escaping_rule, json_info));
    return data_types;
}

void transformInferredTypesByEscapingRuleIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule, JSONInferenceInfo * json_info)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::JSON:
            transformInferredJSONTypesIfNeeded(first, second, settings, json_info);
            break;
        case FormatSettings::EscapingRule::Escaped: [[fallthrough]];
        case FormatSettings::EscapingRule::Raw: [[fallthrough]];
        case FormatSettings::EscapingRule::Quoted: [[fallthrough]];
        case FormatSettings::EscapingRule::CSV:
            transformInferredTypesIfNeeded(first, second, settings);
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot transform inferred types for value with {} escaping rule",
                            escapingRuleToString(escaping_rule));
    }
}


DataTypePtr getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule escaping_rule)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::CSV:
        case FormatSettings::EscapingRule::Escaped:
        case FormatSettings::EscapingRule::Raw:
            return std::make_shared<DataTypeString>();
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

String getAdditionalFormatInfoForAllRowBasedFormats(const FormatSettings & settings)
{
    return fmt::format(
        "schema_inference_hints={}, max_rows_to_read_for_schema_inference={}, max_bytes_to_read_for_schema_inference={}, schema_inference_make_columns_nullable={}",
        settings.schema_inference_hints,
        settings.max_rows_to_read_for_schema_inference,
        settings.max_bytes_to_read_for_schema_inference,
        settings.schema_inference_make_columns_nullable);
}

String getAdditionalFormatInfoByEscapingRule(const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule)
{
    String result = getAdditionalFormatInfoForAllRowBasedFormats(settings);
    /// First, settings that are common for all text formats:
    result += fmt::format(
        ", try_infer_integers={}, try_infer_dates={}, try_infer_datetimes={}, try_infer_datetimes_only_datetime64={}",
        settings.try_infer_integers,
        settings.try_infer_dates,
        settings.try_infer_datetimes,
        settings.try_infer_datetimes_only_datetime64);

    /// Second, format-specific settings:
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Escaped:
        case FormatSettings::EscapingRule::Raw:
            result += fmt::format(
                ", use_best_effort_in_schema_inference={}, bool_true_representation={}, bool_false_representation={}, null_representation={}",
                settings.tsv.use_best_effort_in_schema_inference,
                settings.bool_true_representation,
                settings.bool_false_representation,
                settings.tsv.null_representation);
            break;
        case FormatSettings::EscapingRule::CSV:
            result += fmt::format(
                ", use_best_effort_in_schema_inference={}, bool_true_representation={}, bool_false_representation={},"
                " null_representation={}, delimiter={}, tuple_delimiter={}, try_infer_numbers_from_strings={}, try_infer_strings_from_quoted_tuples={}",
                settings.csv.use_best_effort_in_schema_inference,
                settings.bool_true_representation,
                settings.bool_false_representation,
                settings.csv.null_representation,
                settings.csv.delimiter,
                settings.csv.tuple_delimiter,
                settings.csv.try_infer_numbers_from_strings,
                settings.csv.try_infer_strings_from_quoted_tuples);
            break;
        case FormatSettings::EscapingRule::JSON:
            result += fmt::format(
                ", try_infer_numbers_from_strings={}, read_bools_as_numbers={}, read_bools_as_strings={}, read_objects_as_strings={}, "
                "read_numbers_as_strings={}, "
                "read_arrays_as_strings={}, try_infer_objects_as_tuples={}, infer_incomplete_types_as_strings={}, try_infer_objects={}, "
                "use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects={}",
                settings.json.try_infer_numbers_from_strings,
                settings.json.read_bools_as_numbers,
                settings.json.read_bools_as_strings,
                settings.json.read_objects_as_strings,
                settings.json.read_numbers_as_strings,
                settings.json.read_arrays_as_strings,
                settings.json.try_infer_objects_as_tuples,
                settings.json.infer_incomplete_types_as_strings,
                settings.json.allow_deprecated_object_type,
                settings.json.use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects);
            break;
        default:
            break;
    }

    return result;
}


void checkSupportedDelimiterAfterField(FormatSettings::EscapingRule escaping_rule, const String & delimiter, const DataTypePtr & type)
{
    if (escaping_rule != FormatSettings::EscapingRule::Escaped)
        return;

    bool is_supported_delimiter_after_string = !delimiter.empty() && (delimiter.front() == '\t' || delimiter.front() == '\n');
    if (is_supported_delimiter_after_string)
        return;

    /// Nullptr means that field is skipped and it's equivalent to String
    if (!type || isString(removeNullable(removeLowCardinality(type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'Escaped' serialization requires delimiter after String field to start with '\\t' or '\\n'");
}

}
