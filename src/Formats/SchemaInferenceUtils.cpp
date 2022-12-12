#include <Formats/SchemaInferenceUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/PeekableReadBuffer.h>

#include <Core/Block.h>
#include <Common/assert_cast.h>

namespace DB
{

static bool checkIfTypesAreEqual(const DataTypes & types)
{
    for (size_t i = 1; i < types.size(); ++i)
    {
        if (!types[0]->equals(*types[i]))
            return false;
    }
    return true;
}

/// If we have both Nothing and non Nothing types, convert all Nothing types to the first non Nothing.
/// For example if we have types [Nothing, String, Nothing] we change it to [String, String, String]
static void transformNothingSimpleTypes(DataTypes & data_types)
{
    bool have_nothing = false;
    DataTypePtr not_nothing_type = nullptr;
    for (const auto & type : data_types)
    {
        if (isNothing(type))
            have_nothing = true;
        else if (!not_nothing_type)
            not_nothing_type = type;
    }

    if (have_nothing && not_nothing_type)
    {
        for (auto & type : data_types)
        {
            if (isNothing(type))
                type = not_nothing_type;
        }
    }
}

/// If we have both Int64 and Float64 types, convert all Int64 to Float64.
static void transformIntegersAndFloatsToFloats(DataTypes & data_types)
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

/// If we have only Date and DateTime types, convert Date to DateTime,
/// otherwise, convert all Date and DateTime to String.
static void transformDatesAndDateTimes(DataTypes & data_types)
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

/// If we have numbers (Int64/Float64) and String types and numbers were parsed from String,
/// convert all numbers to String.
static void transformJSONNumbersBackToString(DataTypes & data_types, const FormatSettings & settings, JSONInferenceInfo * json_info)
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
            if (isNumber(type)
                && (settings.json.read_numbers_as_strings || !json_info
                    || json_info->numbers_parsed_from_json_strings.contains(type.get())))
                type = std::make_shared<DataTypeString>();
        }
    }
}

/// If we have both Bool and number (Int64/Float64) types,
/// convert all Bool to Int64/Float64.
static void transformBoolsAndNumbersToNumbers(DataTypes & data_types)
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

/// If we have type Nothing (or Nullable(Nothing) for JSON) and some other non Nothing types,
/// convert all Nothing types to the first non Nothing.
/// For example, when we have [Nothing, Array(Int64)] it will convert it to [Array(Int64), Array(Int64)]
/// (it can happen when transforming complex nested types like [Array(Nothing), Array(Array(Int64))])
template <bool is_json>
static void transformNothingComplexTypes(DataTypes & data_types)
{
    bool have_nothing = false;
    DataTypePtr not_nothing_type = nullptr;
    for (const auto & type : data_types)
    {
        if (isNothing(type) || (is_json && type->onlyNull()))
            have_nothing = true;
        else if (!not_nothing_type)
            not_nothing_type = type;
    }

    if (have_nothing && not_nothing_type)
    {
        for (auto & type : data_types)
        {
            if (isNothing(type) || (is_json && type->onlyNull()))
                type = not_nothing_type;
        }
    }
}

/// If we have both Nullable and non Nullable types, make all types Nullable
static void transformNullableTypes(DataTypes & data_types)
{
    bool have_nullable = false;
    for (const auto & type : data_types)
    {
        if (type->isNullable())
        {
            have_nullable = true;
            break;
        }
    }

    if (have_nullable)
    {
        for (auto & type : data_types)
        {
            if (type->canBeInsideNullable())
                type = makeNullable(type);
        }
    }
}

/// If we have Tuple with the same nested types like Tuple(Int64, Int64),
/// convert it to Array(Int64). It's used for JSON values.
/// For example when we had type Tuple(Int64, Nullable(Nothing)) and we
/// transformed it to Tuple(Nullable(Int64), Nullable(Int64)) we will
/// also transform it to Array(Nullable(Int64))
static void transformTuplesWithEqualNestedTypesToArrays(DataTypes & data_types)
{
    for (auto & type : data_types)
    {
        if (isTuple(type))
        {
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
            if (checkIfTypesAreEqual(tuple_type->getElements()))
                type = std::make_shared<DataTypeArray>(tuple_type->getElements().back());
        }
    }
}

template <bool is_json>
static void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info = nullptr);

/// If we have Tuple and Array types, try to convert them all to Array
/// if there is a common type for all nested types.
/// For example, if we have [Tuple(Nullable(Nothing), String), Array(Date), Tuple(Date, String)]
/// it will convert them all to Array(String)
static void transformJSONTuplesAndArraysToArrays(DataTypes & data_types, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    bool have_arrays = false;
    bool have_tuples = false;
    bool tuple_sizes_are_equal = true;
    size_t tuple_size = 0;
    for (const auto & type : data_types)
    {
        if (isArray(type))
            have_arrays = true;
        else if (isTuple(type))
        {
            have_tuples = true;
            const auto & current_tuple_size = assert_cast<const DataTypeTuple &>(*type).getElements().size();
            if (!tuple_size)
                tuple_size = current_tuple_size;
            else
                tuple_sizes_are_equal &= current_tuple_size == tuple_size;
        }
    }

    if (have_tuples && (have_arrays || !tuple_sizes_are_equal))
    {
        DataTypes nested_types;
        for (auto & type : data_types)
        {
            if (isArray(type))
                nested_types.push_back(assert_cast<const DataTypeArray &>(*type).getNestedType());
            else
            {
                const auto & elements = assert_cast<const DataTypeTuple & >(*type).getElements();
                for (const auto & element : elements)
                    nested_types.push_back(element);
            }
        }

        transformInferredTypesIfNeededImpl<true>(nested_types, settings, json_info);
        if (checkIfTypesAreEqual(nested_types))
        {
            for (auto & type : data_types)
                type = std::make_shared<DataTypeArray>(nested_types.back());
        }
    }
}

/// If we have Map and Object(JSON) types, convert all Map types to Object(JSON).
/// If we have Map types with different value types, convert all Map types to Object(JSON)
static void transformMapsAndObjectsToObjects(DataTypes & data_types)
{
    bool have_maps = false;
    bool have_objects = false;
    bool maps_are_equal = true;
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
                maps_are_equal &= type->equals(*first_map_type);
            }
        }
        else if (isObject(type))
        {
            have_objects = true;
        }
    }

    if (have_maps && (have_objects || !maps_are_equal))
    {
        for (auto & type : data_types)
        {
            if (isMap(type))
                type = std::make_shared<DataTypeObject>("json", true);
        }
    }
}

template <bool is_json>
static void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    auto transform_simple_types = [&](DataTypes & data_types)
    {
        /// Remove all Nothing type if possible.
        transformNothingSimpleTypes(data_types);

        /// Transform integers to floats if needed.
        if (settings.try_infer_integers)
            transformIntegersAndFloatsToFloats(data_types);

        /// Transform Date to DateTime or both to String if needed.
        if (settings.try_infer_dates || settings.try_infer_datetimes)
            transformDatesAndDateTimes(data_types);

        if constexpr (!is_json)
            return;

        /// Check settings specific for JSON formats.

        /// Convert numbers inferred from strings back to strings if needed.
        if (settings.json.try_infer_numbers_from_strings || settings.json.read_numbers_as_strings)
            transformJSONNumbersBackToString(data_types, settings, json_info);

        /// Convert Bool to number (Int64/Float64) if needed.
        if (settings.json.read_bools_as_numbers)
            transformBoolsAndNumbersToNumbers(data_types);
    };

    auto transform_complex_types = [&](DataTypes & data_types)
    {
        /// Make types Nullable if needed.
        transformNullableTypes(data_types);

        /// If we have type Nothing, it means that we had empty Array/Map while inference.
        /// If there is at least one non Nothing type, change all Nothing types to it.
        transformNothingComplexTypes<is_json>(data_types);

        if constexpr (!is_json)
            return;

        /// Convert JSON tuples with same nested types to arrays.
        transformTuplesWithEqualNestedTypesToArrays(data_types);

        /// Convert JSON tuples and arrays to arrays if possible.
        transformJSONTuplesAndArraysToArrays(data_types, settings, json_info);

        /// Convert Maps to Objects if needed.
        if (settings.json.try_infer_objects)
            transformMapsAndObjectsToObjects(data_types);
    };

    transformTypesRecursively(types, transform_simple_types, transform_complex_types);
}

void transformInferredTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<false>(types, settings, nullptr);
    first = types[0];
    second = types[1];
}

void transformInferredJSONTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<true>(types, settings, json_info);
    first = types[0];
    second = types[1];
}

void transformJSONTupleToArrayIfPossible(DataTypePtr & data_type, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    if (!isTuple(data_type))
        return;

    const auto * tuple_type = assert_cast<const DataTypeTuple *>(data_type.get());
    auto nested_types = tuple_type->getElements();
    transformInferredTypesIfNeededImpl<true>(nested_types, settings, json_info);
    if (checkIfTypesAreEqual(nested_types))
        data_type = std::make_shared<DataTypeArray>(nested_types.back());
}


template <bool is_json>
static DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info);

static bool tryInferDate(const std::string_view & field)
{
    ReadBufferFromString buf(field);
    DayNum tmp;
    return tryReadDateText(tmp, buf) && buf.eof();
}

static bool tryInferDateTime(const std::string_view & field, const FormatSettings & settings)
{
    if (field.empty())
        return false;

    ReadBufferFromString buf(field);
    Float64 tmp_float;
    /// Check if it's just a number, and if so, don't try to infer DateTime from it,
    /// because we can interpret this number as a timestamp and it will lead to
    /// inferring DateTime instead of simple Int64/Float64 in some cases.
    if (tryReadFloatText(tmp_float, buf) && buf.eof())
        return false;

    buf.seek(0, SEEK_SET); /// Return position to the beginning
    DateTime64 tmp;
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            if (tryReadDateTime64Text(tmp, 9, buf) && buf.eof())
                return true;
            break;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            if (tryParseDateTime64BestEffort(tmp, 9, buf, DateLUT::instance(), DateLUT::instance("UTC")) && buf.eof())
                return true;
            break;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            if (tryParseDateTime64BestEffortUS(tmp, 9, buf, DateLUT::instance(), DateLUT::instance("UTC")) && buf.eof())
                return true;
            break;
    }

    return false;
}

DataTypePtr tryInferDateOrDateTimeFromString(const std::string_view & field, const FormatSettings & settings)
{
    if (settings.try_infer_dates && tryInferDate(field))
        return std::make_shared<DataTypeDate>();

    if (settings.try_infer_datetimes && tryInferDateTime(field, settings))
        return std::make_shared<DataTypeDateTime64>(9);

    return nullptr;
}

template <bool is_json>
static DataTypePtr tryInferArray(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    assertChar('[', buf);
    skipWhitespaceIfAny(buf);

    DataTypes nested_types;
    bool first = true;
    while (!buf.eof() && *buf.position() != ']')
    {
        if (!first)
        {
            /// Skip field delimiter between array elements.
            skipWhitespaceIfAny(buf);
            if (!checkChar(',', buf))
                return nullptr;
            skipWhitespaceIfAny(buf);
        }
        else
            first = false;

        auto nested_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info);
        /// If we couldn't infer element type, array type also cannot be inferred.
        if (!nested_type)
            return nullptr;

        nested_types.push_back(nested_type);

        skipWhitespaceIfAny(buf);
    }

    /// No ']' at the end of array
    if (buf.eof())
        return nullptr;

    assertChar(']', buf);
    skipWhitespaceIfAny(buf);

    /// Empty array has type Array(Nothing)
    if (nested_types.empty())
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());

    if (checkIfTypesAreEqual(nested_types))
        return std::make_shared<DataTypeArray>(std::move(nested_types.back()));

    /// If element types are not equal, we should try to find common type.
    /// If after transformation element types are still different, we return Tuple for JSON and
    /// nullptr for other formats (nullptr means we couldn't infer the type).
    if constexpr (is_json)
    {
        /// For JSON if we have not complete types, we should not try to transform them
        /// and return it as a Tuple.
        /// For example, if we have types [Float64, Nullable(Nothing), Float64]
        /// it can be Array(Float64) or Tuple(Float64, <some_type>, Float64) and
        /// we can't determine which one it is. But we will be able to do it later
        /// when we will have types from other rows for this column.
        /// For example, if in the next row we will have types [Nullable(Nothing), String, Float64],
        /// we can determine the type for this column as Tuple(Nullable(Float64), Nullable(String), Float64).
        for (const auto & type : nested_types)
        {
            if (!checkIfTypeIsComplete(type))
                return std::make_shared<DataTypeTuple>(nested_types);
        }

        auto nested_types_copy = nested_types;
        transformInferredTypesIfNeededImpl<is_json>(nested_types_copy, settings, json_info);

        if (checkIfTypesAreEqual(nested_types_copy))
            return std::make_shared<DataTypeArray>(nested_types_copy.back());

        return std::make_shared<DataTypeTuple>(nested_types);
    }
    else
    {
        transformInferredTypesIfNeededImpl<is_json>(nested_types, settings);
        if (checkIfTypesAreEqual(nested_types))
            return std::make_shared<DataTypeArray>(nested_types.back());

        /// We couldn't determine common type for array element.
        return nullptr;
    }
}

static DataTypePtr tryInferTuple(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    assertChar('(', buf);
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

        auto nested_type = tryInferDataTypeForSingleFieldImpl<false>(buf, settings, json_info);
        /// If we couldn't infer element type, tuple type also cannot be inferred.
        if (!nested_type)
            return nullptr;

        nested_types.push_back(nested_type);
        skipWhitespaceIfAny(buf);
    }

    if (buf.eof() || nested_types.empty())
        return nullptr;

    assertChar(')', buf);
    skipWhitespaceIfAny(buf);

    return std::make_shared<DataTypeTuple>(nested_types);
}

template <bool check_eof>
static DataTypePtr tryInferNumberFromStringBuffer(ReadBufferFromString & buf, const FormatSettings & settings)
{
    if (settings.try_infer_integers)
    {
        Int64 tmp_int;
        if (tryReadIntText(tmp_int, buf) && (!check_eof || buf.eof()))
            return std::make_shared<DataTypeInt64>();
    }

    /// We cam safely get back to the start of buffer, because we read from a string and we didn't reach eof.
    buf.position() = buf.buffer().begin();

    Float64 tmp;
    if (tryReadFloatText(tmp, buf) && (!check_eof || buf.eof()))
        return std::make_shared<DataTypeFloat64>();

    return nullptr;
}

static DataTypePtr tryInferNumber(ReadBuffer & buf, const FormatSettings & settings)
{
    /// If we read from String, we can do it in a more efficient way.
    if (auto * string_buf = dynamic_cast<ReadBufferFromString *>(&buf))
        return tryInferNumberFromStringBuffer<false>(*string_buf, settings);

    Float64 tmp_float;
    if (settings.try_infer_integers)
    {
        /// We should use PeekableReadBuffer, because we need to
        /// rollback to the start of number to parse it as integer first
        /// and then as float.
        PeekableReadBuffer peekable_buf(buf);
        PeekableReadBufferCheckpoint checkpoint(peekable_buf);
        Int64 tmp_int;
        bool read_int = tryReadIntText(tmp_int, peekable_buf);
        auto * int_end = peekable_buf.position();
        peekable_buf.rollbackToCheckpoint(true);
        if (tryReadFloatText(tmp_float, peekable_buf))
        {
            /// Float parsing reads no fewer bytes than integer parsing,
            /// so position of the buffer is either the same, or further.
            /// If it's the same, then it's integer.
            if (read_int && peekable_buf.position() == int_end)
                return std::make_shared<DataTypeInt64>();
            return std::make_shared<DataTypeFloat64>();
        }
    }
    else if (tryReadFloatText(tmp_float, buf))
    {
        return std::make_shared<DataTypeFloat64>();
    }

    /// This is not a number.
    return nullptr;
}

DataTypePtr tryInferNumberFromString(const std::string_view & field, const FormatSettings & settings)
{
    ReadBufferFromString buf(field);
    return tryInferNumberFromStringBuffer<true>(buf, settings);
}

template <bool is_json>
static DataTypePtr tryInferString(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    String field;
    bool ok = true;
    if constexpr (is_json)
        ok = tryReadJSONStringInto(field, buf);
    else
        ok = tryReadQuotedStringInto(field, buf);

    if (!ok)
        return nullptr;

    skipWhitespaceIfAny(buf);

    /// If it's object key, we should just return String type.
    if constexpr (is_json)
    {
        if (json_info->is_object_key)
            return std::make_shared<DataTypeString>();
    }

    if (auto type = tryInferDateOrDateTimeFromString(field, settings))
        return type;

    if constexpr (is_json)
    {
        if (settings.json.try_infer_numbers_from_strings)
        {
            auto number_type = tryInferNumberFromString(field, settings);
            if (number_type)
            {
                json_info->numbers_parsed_from_json_strings.insert(number_type.get());
                return number_type;
            }
        }
    }

    return std::make_shared<DataTypeString>();
}

template <bool is_json>
static DataTypePtr tryInferMapOrObject(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    assertChar('{', buf);
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

        DataTypePtr key_type;
        if constexpr (is_json)
        {
            /// For JSON key type must be String.
            json_info->is_object_key = true;
            key_type = tryInferString<is_json>(buf, settings, json_info);
            json_info->is_object_key = false;
        }
        else
        {
            key_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, nullptr);
        }

        /// If we couldn't infer key type, we cannot infer Map/JSON object type.
        if (!key_type)
            return nullptr;

        key_types.push_back(key_type);

        skipWhitespaceIfAny(buf);
        if (!checkChar(':', buf))
            return nullptr;
        skipWhitespaceIfAny(buf);

        /// If we couldn't infer element type, Map type also cannot be inferred.
        auto value_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info);
        if (!value_type)
            return nullptr;

        value_types.push_back(value_type);
        skipWhitespaceIfAny(buf);
    }

    if (buf.eof())
        return nullptr;

    assertChar('}', buf);
    skipWhitespaceIfAny(buf);

    if (key_types.empty())
    {
        if constexpr (is_json)
        {
            if (settings.json.try_infer_objects)
                return std::make_shared<DataTypeObject>("json", true);
        }
        /// Empty Map is Map(Nothing, Nothing)
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
    }

    if constexpr (is_json)
    {
        /// If it's JSON field and one of value types is JSON Object, return also JSON Object.
        for (const auto & value_type : value_types)
        {
            if (isObject(value_type))
                return std::make_shared<DataTypeObject>("json", true);
        }

        transformInferredTypesIfNeededImpl<is_json>(value_types, settings, json_info);
        if (!checkIfTypesAreEqual(value_types))
        {
            if (settings.json.try_infer_objects)
                return std::make_shared<DataTypeObject>("json", true);
            return nullptr;
        }

        return std::make_shared<DataTypeMap>(key_types.back(), value_types.back());
    }

    if (!checkIfTypesAreEqual(key_types))
        transformInferredTypesIfNeededImpl<is_json>(key_types, settings);
    if (!checkIfTypesAreEqual(value_types))
        transformInferredTypesIfNeededImpl<is_json>(value_types, settings);

    if (!checkIfTypesAreEqual(key_types) || !checkIfTypesAreEqual(value_types))
        return nullptr;

    auto key_type = removeNullable(key_types.back());
    if (!DataTypeMap::checkKeyType(key_type))
        return nullptr;

    return std::make_shared<DataTypeMap>(key_type, value_types.back());
}

template <bool is_json>
static DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    skipWhitespaceIfAny(buf);

    if (buf.eof())
        return nullptr;

    /// Array [field1, field2, ...]
    if (*buf.position() == '[')
        return tryInferArray<is_json>(buf, settings, json_info);

    /// Tuple (field1, field2, ...), if format is not JSON
    if constexpr (!is_json)
    {
        if (*buf.position() == '(')
            return tryInferTuple(buf, settings, json_info);
    }

    /// Map/Object for JSON { key1 : value1, key2 : value2, ...}
    if (*buf.position() == '{')
        return tryInferMapOrObject<is_json>(buf, settings, json_info);

    /// String
    char quote = is_json ? '"' : '\'';
    if (*buf.position() == quote)
        return tryInferString<is_json>(buf, settings, json_info);

    /// Bool
    if (checkStringCaseInsensitive("true", buf) || checkStringCaseInsensitive("false", buf))
        return DataTypeFactory::instance().get("Bool");

    /// Null or NaN
    if (checkCharCaseInsensitive('n', buf))
    {
        if (checkStringCaseInsensitive("ull", buf))
            return makeNullable(std::make_shared<DataTypeNothing>());
        else if (checkStringCaseInsensitive("an", buf))
            return std::make_shared<DataTypeFloat64>();
    }

    /// Number
    return tryInferNumber(buf, settings);
}

DataTypePtr tryInferDataTypeForSingleField(ReadBuffer & buf, const FormatSettings & settings)
{
    return tryInferDataTypeForSingleFieldImpl<false>(buf, settings, nullptr);
}

DataTypePtr tryInferDataTypeForSingleField(std::string_view field, const FormatSettings & settings)
{
    ReadBufferFromString buf(field);
    auto type = tryInferDataTypeForSingleFieldImpl<false>(buf, settings, nullptr);
    /// Check if there is no unread data in buffer.
    if (!buf.eof())
        return nullptr;
    return type;
}

DataTypePtr tryInferDataTypeForSingleJSONField(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    return tryInferDataTypeForSingleFieldImpl<true>(buf, settings, json_info);
}

DataTypePtr tryInferDataTypeForSingleJSONField(std::string_view field, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    ReadBufferFromString buf(field);
    auto type = tryInferDataTypeForSingleFieldImpl<true>(buf, settings, json_info);
    /// Check if there is no unread data in buffer.
    if (!buf.eof())
        return nullptr;
    return type;
}

DataTypePtr makeNullableRecursively(DataTypePtr type)
{
    if (!type)
        return nullptr;

    WhichDataType which(type);

    if (which.isNullable())
        return type;

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = makeNullableRecursively(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = makeNullableRecursively(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }
        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursively(map_type->getKeyType());
        auto value_type = makeNullableRecursively(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCarnality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursively(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    return makeNullable(type);
}

NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header)
{
    NamesAndTypesList result;
    for (auto & [name, type] : header.getNamesAndTypesList())
        result.emplace_back(name, makeNullableRecursively(type));
    return result;
}

bool checkIfTypeIsComplete(const DataTypePtr & type)
{
    if (!type)
        return false;

    WhichDataType which(type);

    if (which.isNothing())
        return false;

    if (which.isNullable())
        return checkIfTypeIsComplete(assert_cast<const DataTypeNullable *>(type.get())->getNestedType());

    if (which.isArray())
        return checkIfTypeIsComplete(assert_cast<const DataTypeArray *>(type.get())->getNestedType());

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        for (const auto & element : tuple_type->getElements())
        {
            if (!checkIfTypeIsComplete(element))
                return false;
        }
        return true;
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        if (!checkIfTypeIsComplete(map_type->getKeyType()))
            return false;
        return checkIfTypeIsComplete(map_type->getValueType());
    }

    return true;
}

}
