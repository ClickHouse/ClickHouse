#include <Formats/EscapingRuleUtils.h>
#include <Formats/JSONUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/transformTypesRecursively.h>
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
    NullOutput out;
    constexpr const char * field_name = "<SKIPPED COLUMN>";
    constexpr size_t field_name_len = 16;
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::None:
            /// Empty field, just skip spaces
            break;
        case FormatSettings::EscapingRule::Escaped:
            readEscapedStringInto(out, buf);
            break;
        case FormatSettings::EscapingRule::Quoted:
            readQuotedFieldInto(out, buf);
            break;
        case FormatSettings::EscapingRule::CSV:
            readCSVStringInto(out, buf, format_settings.csv);
            break;
        case FormatSettings::EscapingRule::JSON:
            skipJSONField(buf, StringRef(field_name, field_name_len));
            break;
        case FormatSettings::EscapingRule::Raw:
            readStringInto(out, buf);
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
                readQuotedField(result, buf);
            break;
        case FormatSettings::EscapingRule::JSON:
            if constexpr (read_string)
                readJSONString(result, buf);
            else
                readJSONField(result, buf);
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

void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, bool is_json, const std::unordered_set<const IDataType *> * numbers_parsed_from_json_strings = nullptr)
{
    /// Do nothing if we didn't try to infer something special.
    if (!settings.try_infer_integers && !settings.try_infer_dates && !settings.try_infer_datetimes && !is_json)
        return;

    auto transform_simple_types = [&](DataTypes & data_types)
    {
        /// If we have floats and integers convert them all to float.
        if (settings.try_infer_integers)
        {
            bool have_floats = false;
            bool have_integers = false;
            for (const auto & type : data_types)
            {
                have_floats |= isFloat(type);
                have_integers |= isInteger(type) && !isBool(type);
            }

            if (have_floats && have_integers)
            {
                for (auto & type : data_types)
                {
                    if (isInteger(type))
                        type = std::make_shared<DataTypeFloat64>();
                }
            }
        }

        /// If we have date/datetimes and smth else, convert them to string.
        /// If we have only dates and datetimes, convert dates to datetime.
        if (settings.try_infer_dates || settings.try_infer_datetimes)
        {
            bool have_dates = false;
            bool have_datetimes = false;
            bool all_dates_or_datetimes = true;

            for (const auto & type : data_types)
            {
                have_dates |= isDate(type);
                have_datetimes |= isDateTime64(type);
                all_dates_or_datetimes &= isDate(type) || isDateTime64(type);
            }

            if (!all_dates_or_datetimes && (have_dates || have_datetimes))
            {
                for (auto & type : data_types)
                {
                    if (isDate(type) || isDateTime64(type))
                        type = std::make_shared<DataTypeString>();
                }
            }
            else if (have_dates && have_datetimes)
            {
                for (auto & type : data_types)
                {
                    if (isDate(type))
                        type = std::make_shared<DataTypeDateTime64>(9);
                }
            }
        }

        if (!is_json)
            return;

        /// Check settings specific for JSON formats.

        /// If we have numbers and strings, convert numbers to strings.
        /// (Actually numbers could not be parsed from
        if (settings.json.try_infer_numbers_from_strings)
        {
            bool have_strings = false;
            bool have_numbers = false;
            for (const auto & type : data_types)
            {
                have_strings |= isString(type);
                have_numbers |= isNumber(type);
            }

            if (have_strings && have_numbers)
            {
                for (auto & type : data_types)
                {
                    if (isNumber(type) && (!numbers_parsed_from_json_strings || numbers_parsed_from_json_strings->contains(type.get())))
                        type = std::make_shared<DataTypeString>();
                }
            }
        }

        if (settings.json.read_bools_as_numbers)
        {
            bool have_floats = false;
            bool have_integers = false;
            bool have_bools = false;
            for (const auto & type : data_types)
            {
                have_floats |= isFloat(type);
                have_integers |= isInteger(type) && !isBool(type);
                have_bools |= isBool(type);
            }

            if (have_bools && (have_integers || have_floats))
            {
                for (auto & type : data_types)
                {
                    if (isBool(type))
                    {
                        if (have_integers)
                            type = std::make_shared<DataTypeInt64>();
                        else
                            type = std::make_shared<DataTypeFloat64>();
                    }
                }
            }
        }
    };

    auto transform_complex_types = [&](DataTypes & data_types)
    {
        if (!is_json)
            return;

        bool have_maps = false;
        bool have_objects = false;
        bool are_maps_equal = true;
        DataTypePtr first_map_type;
        for (const auto & type : data_types)
        {
            if (isMap(type))
            {
                if (!have_maps)
                {
                    first_map_type = type;
                    have_maps = true;
                }
                else
                {
                    are_maps_equal &= type->equals(*first_map_type);
                }
            }
            else if (isObject(type))
            {
                have_objects = true;
            }
        }

        if (have_maps && (have_objects || !are_maps_equal))
        {
            for (auto & type : data_types)
            {
                if (isMap(type))
                    type = std::make_shared<DataTypeObject>("json", true);
            }
        }
    };

    transformTypesRecursively(types, transform_simple_types, transform_complex_types);
}

void transformInferredTypesIfNeeded(DataTypes & types, const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule)
{
    transformInferredTypesIfNeededImpl(types, settings, escaping_rule == FormatSettings::EscapingRule::JSON);
}

void transformInferredTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, FormatSettings::EscapingRule escaping_rule)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeeded(types, settings, escaping_rule);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

void transformInferredJSONTypesIfNeeded(DataTypes & types, const FormatSettings & settings, const std::unordered_set<const IDataType *> * numbers_parsed_from_json_strings)
{
    transformInferredTypesIfNeededImpl(types, settings, true, numbers_parsed_from_json_strings);
}

void transformInferredJSONTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings)
{
    DataTypes types = {first, second};
    transformInferredJSONTypesIfNeeded(types, settings);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

DataTypePtr tryInferDateOrDateTime(const std::string_view & field, const FormatSettings & settings)
{
    if (settings.try_infer_dates)
    {
        ReadBufferFromString buf(field);
        DayNum tmp;
        if (tryReadDateText(tmp, buf) && buf.eof())
            return makeNullable(std::make_shared<DataTypeDate>());
    }

    if (settings.try_infer_datetimes)
    {
        ReadBufferFromString buf(field);
        DateTime64 tmp;
        if (tryReadDateTime64Text(tmp, 9, buf) && buf.eof())
            return makeNullable(std::make_shared<DataTypeDateTime64>(9));
    }

    return nullptr;
}

static DataTypePtr determineDataTypeForSingleFieldImpl(ReadBufferFromString & buf, const FormatSettings & settings)
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

            auto nested_type = determineDataTypeForSingleFieldImpl(buf, settings);
            if (!nested_type)
                return nullptr;

            nested_types.push_back(nested_type);
        }

        if (buf.eof())
            return nullptr;

        ++buf.position();

        if (nested_types.empty())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());

        transformInferredTypesIfNeeded(nested_types, settings);

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

            auto nested_type = determineDataTypeForSingleFieldImpl(buf, settings);
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

            auto key_type = determineDataTypeForSingleFieldImpl(buf, settings);
            if (!key_type)
                return nullptr;

            key_types.push_back(key_type);

            skipWhitespaceIfAny(buf);
            if (!checkChar(':', buf))
                return nullptr;
            skipWhitespaceIfAny(buf);

            auto value_type = determineDataTypeForSingleFieldImpl(buf, settings);
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

        transformInferredTypesIfNeeded(key_types, settings);
        transformInferredTypesIfNeeded(value_types, settings);

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
        String field;
        while (!buf.eof())
        {
            char * next_pos = find_first_symbols<'\\', '\''>(buf.position(), buf.buffer().end());
            field.append(buf.position(), next_pos);
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            if (*buf.position() == '\'')
                break;

            field.push_back(*buf.position());
            if (*buf.position() == '\\')
                ++buf.position();
        }

        if (buf.eof())
            return nullptr;

        ++buf.position();
        if (auto type = tryInferDateOrDateTime(field, settings))
            return type;

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
    auto * pos_before_float = buf.position();
    if (tryReadFloatText(tmp, buf))
    {
        if (settings.try_infer_integers)
        {
            auto * float_end_pos = buf.position();
            buf.position() = pos_before_float;
            Int64 tmp_int;
            if (tryReadIntText(tmp_int, buf) && buf.position() == float_end_pos)
                return std::make_shared<DataTypeInt64>();

            buf.position() = float_end_pos;
        }

        return std::make_shared<DataTypeFloat64>();
    }

    return nullptr;
}

static DataTypePtr determineDataTypeForSingleField(ReadBufferFromString & buf, const FormatSettings & settings)
{
    return makeNullableRecursivelyAndCheckForNothing(determineDataTypeForSingleFieldImpl(buf, settings));
}

DataTypePtr determineDataTypeByEscapingRule(const String & field, const FormatSettings & format_settings, FormatSettings::EscapingRule escaping_rule)
{
    switch (escaping_rule)
    {
        case FormatSettings::EscapingRule::Quoted:
        {
            ReadBufferFromString buf(field);
            auto type = determineDataTypeForSingleField(buf, format_settings);
            return buf.eof() ? type : nullptr;
        }
        case FormatSettings::EscapingRule::JSON:
            return JSONUtils::getDataTypeFromField(field, format_settings);
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
                auto data = std::string_view(field.data() + 1, field.size() - 2);
                if (auto date_type = tryInferDateOrDateTime(data, format_settings))
                    return date_type;

                ReadBufferFromString buf(data);
                /// Try to determine the type of value inside quotes
                auto type = determineDataTypeForSingleField(buf, format_settings);

                if (!type)
                    return nullptr;

                /// If it's a number or tuple in quotes or there is some unread data in buffer, we determine it as a string.
                if (isNumber(removeNullable(type)) || isTuple(type) || !buf.eof())
                    return makeNullable(std::make_shared<DataTypeString>());

                return type;
            }

            /// Case when CSV value is not in quotes. Check if it's a number, and if not, determine it's as a string.
            if (format_settings.try_infer_integers)
            {
                ReadBufferFromString buf(field);
                Int64 tmp_int;
                if (tryReadIntText(tmp_int, buf) && buf.eof())
                    return makeNullable(std::make_shared<DataTypeInt64>());
            }

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

            if (auto date_type = tryInferDateOrDateTime(field, format_settings))
                return date_type;

            ReadBufferFromString buf(field);
            auto type = determineDataTypeForSingleField(buf, format_settings);
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
        case FormatSettings::EscapingRule::CSV:
        case FormatSettings::EscapingRule::Escaped:
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
