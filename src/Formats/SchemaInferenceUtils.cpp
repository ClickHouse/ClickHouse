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

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
}

namespace
{
    bool checkIfTypesAreEqual(const DataTypes & types)
    {
        if (types.empty())
            return true;

        for (size_t i = 1; i < types.size(); ++i)
        {
            if (!types[0]->equals(*types[i]))
                return false;
        }
        return true;
    }

    void updateTypeIndexes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        type_indexes.clear();
        for (const auto & type : data_types)
            type_indexes.insert(type->getTypeId());
    }

    /// If we have both Nothing and non Nothing types, convert all Nothing types to the first non Nothing.
    /// For example if we have types [Nothing, String, Nothing] we change it to [String, String, String]
    void transformNothingSimpleTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        /// Check if we have both Nothing and non Nothing types.
        if (!type_indexes.contains(TypeIndex::Nothing) || type_indexes.size() <= 1)
            return;

        DataTypePtr not_nothing_type = nullptr;
        for (const auto & type : data_types)
        {
            if (!isNothing(type))
            {
                not_nothing_type = type;
                break;
            }
        }

        for (auto & type : data_types)
        {
            if (isNothing(type))
                type = not_nothing_type;
        }

        type_indexes.erase(TypeIndex::Nothing);
    }

    /// If we have both Int64 and UInt64, convert all Int64 to UInt64,
    /// because UInt64 is inferred only in case of Int64 overflow.
    void transformIntegers(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Int64) || !type_indexes.contains(TypeIndex::UInt64))
            return;

        for (auto & type : data_types)
        {
            if (WhichDataType(type).isInt64())
                type = std::make_shared<DataTypeUInt64>();
        }

        type_indexes.erase(TypeIndex::Int64);
    }

    /// If we have both Int64 and Float64 types, convert all Int64 to Float64.
    void transformIntegersAndFloatsToFloats(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_floats = type_indexes.contains(TypeIndex::Float64);
        bool have_integers = type_indexes.contains(TypeIndex::Int64) || type_indexes.contains(TypeIndex::UInt64);
        if (!have_integers || !have_floats)
            return;

        for (auto & type : data_types)
        {
            WhichDataType which(type);
            if (which.isInt64() || which.isUInt64())
                type = std::make_shared<DataTypeFloat64>();
        }

        type_indexes.erase(TypeIndex::Int64);
        type_indexes.erase(TypeIndex::UInt64);
    }

    /// If we have only Date and DateTime types, convert Date to DateTime,
    /// otherwise, convert all Date and DateTime to String.
    void transformDatesAndDateTimes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_dates = type_indexes.contains(TypeIndex::Date);
        bool have_datetimes = type_indexes.contains(TypeIndex::DateTime64);
        bool all_dates_or_datetimes = (type_indexes.size() == (static_cast<size_t>(have_dates) + static_cast<size_t>(have_datetimes)));

        if (!all_dates_or_datetimes && (have_dates || have_datetimes))
        {
            for (auto & type : data_types)
            {
                if (isDate(type) || isDateTime64(type))
                    type = std::make_shared<DataTypeString>();
            }

            type_indexes.erase(TypeIndex::Date);
            type_indexes.erase(TypeIndex::DateTime);
            type_indexes.insert(TypeIndex::String);
            return;
        }

        if (have_dates && have_datetimes)
        {
            for (auto & type : data_types)
            {
                if (isDate(type))
                    type = std::make_shared<DataTypeDateTime64>(9);
            }

            type_indexes.erase(TypeIndex::Date);
        }
    }

    /// If we have numbers (Int64/UInt64/Float64) and String types and numbers were parsed from String,
    /// convert all numbers to String.
    void transformJSONNumbersBackToString(
        DataTypes & data_types, const FormatSettings & settings, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        bool have_strings = type_indexes.contains(TypeIndex::String);
        bool have_numbers = type_indexes.contains(TypeIndex::Int64) || type_indexes.contains(TypeIndex::UInt64) || type_indexes.contains(TypeIndex::Float64);
        if (!have_strings || !have_numbers)
            return;

        for (auto & type : data_types)
        {
            if (isNumber(type)
                && (settings.json.read_numbers_as_strings || !json_info
                    || json_info->numbers_parsed_from_json_strings.contains(type.get())))
                type = std::make_shared<DataTypeString>();
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have both Bool and number (Int64/UInt64/Float64) types,
    /// convert all Bool to Int64/UInt64/Float64.
    void transformBoolsAndNumbersToNumbers(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_floats = type_indexes.contains(TypeIndex::Float64);
        bool have_signed_integers = type_indexes.contains(TypeIndex::Int64);
        bool have_unsigned_integers = type_indexes.contains(TypeIndex::UInt64);
        bool have_bools = type_indexes.contains(TypeIndex::UInt8);
        /// Check if we have both Bool and Integer/Float.
        if (!have_bools || (!have_signed_integers && !have_unsigned_integers && !have_floats))
            return;

        for (auto & type : data_types)
        {
            if (isBool(type))
            {
                if (have_signed_integers)
                    type = std::make_shared<DataTypeInt64>();
                else if (have_unsigned_integers)
                    type = std::make_shared<DataTypeUInt64>();
                else
                    type = std::make_shared<DataTypeFloat64>();
            }
        }

        type_indexes.erase(TypeIndex::UInt8);
    }

    /// If we have type Nothing/Nullable(Nothing) and some other non Nothing types,
    /// convert all Nothing/Nullable(Nothing) types to the first non Nothing.
    /// For example, when we have [Nothing, Array(Int64)] it will convert it to [Array(Int64), Array(Int64)]
    /// (it can happen when transforming complex nested types like [Array(Nothing), Array(Array(Int64))])
    void transformNothingComplexTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_nothing = false;
        DataTypePtr not_nothing_type = nullptr;
        for (const auto & type : data_types)
        {
            if (isNothing(removeNullable(type)))
                have_nothing = true;
            else
                not_nothing_type = type;
        }

        if (!have_nothing || !not_nothing_type)
            return;

        for (auto & type : data_types)
        {
            if (isNothing(removeNullable(type)))
                type = not_nothing_type;
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have both Nullable and non Nullable types, make all types Nullable
    void transformNullableTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Nullable))
            return;

        for (auto & type : data_types)
        {
            if (type->canBeInsideNullable())
                type = makeNullable(type);
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have Tuple with the same nested types like Tuple(Int64, Int64),
    /// convert it to Array(Int64). It's used for JSON values.
    /// For example when we had type Tuple(Int64, Nullable(Nothing)) and we
    /// transformed it to Tuple(Nullable(Int64), Nullable(Int64)) we will
    /// also transform it to Array(Nullable(Int64))
    void transformTuplesWithEqualNestedTypesToArrays(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Tuple))
            return;

        bool remove_tuple_index = true;
        for (auto & type : data_types)
        {
            if (isTuple(type))
            {
                const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
                if (checkIfTypesAreEqual(tuple_type->getElements()))
                    type = std::make_shared<DataTypeArray>(tuple_type->getElements().back());
                else
                    remove_tuple_index = false;
            }
        }

        if (remove_tuple_index)
            type_indexes.erase(TypeIndex::Tuple);
    }

    template <bool is_json>
    void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info = nullptr);

    /// If we have Tuple and Array types, try to convert them all to Array
    /// if there is a common type for all nested types.
    /// For example, if we have [Tuple(Nullable(Nothing), String), Array(Date), Tuple(Date, String)]
    /// it will convert them all to Array(String)
    void transformJSONTuplesAndArraysToArrays(
        DataTypes & data_types, const FormatSettings & settings, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        if (!type_indexes.contains(TypeIndex::Tuple))
            return;

        bool have_arrays = type_indexes.contains(TypeIndex::Array);
        bool tuple_sizes_are_equal = true;
        size_t tuple_size = 0;
        for (const auto & type : data_types)
        {
            if (isTuple(type))
            {
                const auto & current_tuple_size = assert_cast<const DataTypeTuple &>(*type).getElements().size();
                if (!tuple_size)
                    tuple_size = current_tuple_size;
                else
                    tuple_sizes_are_equal &= current_tuple_size == tuple_size;
            }
        }

        /// Check if we have arrays and tuples with same size.
        if (!have_arrays && !tuple_sizes_are_equal)
            return;

        DataTypes nested_types;
        for (auto & type : data_types)
        {
            if (isArray(type))
                nested_types.push_back(assert_cast<const DataTypeArray &>(*type).getNestedType());
            else if (isTuple(type))
            {
                const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
                for (const auto & element : elements)
                    nested_types.push_back(element);
            }
        }

        transformInferredTypesIfNeededImpl<true>(nested_types, settings, json_info);
        if (checkIfTypesAreEqual(nested_types))
        {
            for (auto & type : data_types)
            {
                if (isArray(type) || isTuple(type))
                    type = std::make_shared<DataTypeArray>(nested_types.back());
            }

            type_indexes.erase(TypeIndex::Tuple);
        }
    }

    /// If we have Map and Object(JSON) types, convert all Map types to Object(JSON).
    /// If we have Map types with different value types, convert all Map types to Object(JSON)
    void transformMapsAndObjectsToObjects(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Map))
            return;

        bool have_objects = type_indexes.contains(TypeIndex::Object);
        bool maps_are_equal = true;
        DataTypePtr first_map_type = nullptr;
        for (const auto & type : data_types)
        {
            if (isMap(type))
            {
                if (!first_map_type)
                    first_map_type = type;
                else
                    maps_are_equal &= type->equals(*first_map_type);
            }
        }

        if (!have_objects && maps_are_equal)
            return;

        for (auto & type : data_types)
        {
            if (isMap(type))
                type = std::make_shared<DataTypeObject>("json", true);
        }

        type_indexes.erase(TypeIndex::Map);
    }

    void transformMapsObjectsAndStringsToStrings(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_maps = type_indexes.contains(TypeIndex::Map);
        bool have_objects = type_indexes.contains(TypeIndex::Object);
        bool have_strings = type_indexes.contains(TypeIndex::String);

        /// Check if we have both String and Map/Object
        if (!have_strings || (!have_maps && !have_objects))
            return;

        for (auto & type : data_types)
        {
            if (isMap(type) || isObject(type))
                type = std::make_shared<DataTypeString>();
        }

        type_indexes.erase(TypeIndex::Map);
        type_indexes.erase(TypeIndex::Object);
    }

    template <bool is_json>
    void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        auto transform_simple_types = [&](DataTypes & data_types, TypeIndexesSet & type_indexes)
        {
            /// Remove all Nothing type if possible.
            transformNothingSimpleTypes(data_types, type_indexes);

            if (settings.try_infer_integers)
            {
                /// Transform Int64 to UInt64 if needed.
                transformIntegers(data_types, type_indexes);
                /// Transform integers to floats if needed.
                transformIntegersAndFloatsToFloats(data_types, type_indexes);
            }

            /// Transform Date to DateTime or both to String if needed.
            if (settings.try_infer_dates || settings.try_infer_datetimes)
                transformDatesAndDateTimes(data_types, type_indexes);

            if constexpr (!is_json)
                return;

            /// Check settings specific for JSON formats.

            /// Convert numbers inferred from strings back to strings if needed.
            if (settings.json.try_infer_numbers_from_strings || settings.json.read_numbers_as_strings)
                transformJSONNumbersBackToString(data_types, settings, type_indexes, json_info);

            /// Convert Bool to number (Int64/Float64) if needed.
            if (settings.json.read_bools_as_numbers)
                transformBoolsAndNumbersToNumbers(data_types, type_indexes);
        };

        auto transform_complex_types = [&](DataTypes & data_types, TypeIndexesSet & type_indexes)
        {
            /// Make types Nullable if needed.
            transformNullableTypes(data_types, type_indexes);

            /// If we have type Nothing, it means that we had empty Array/Map while inference.
            /// If there is at least one non Nothing type, change all Nothing types to it.
            transformNothingComplexTypes(data_types, type_indexes);

            if constexpr (!is_json)
                return;

            /// Convert JSON tuples with same nested types to arrays.
            transformTuplesWithEqualNestedTypesToArrays(data_types, type_indexes);

            /// Convert JSON tuples and arrays to arrays if possible.
            transformJSONTuplesAndArraysToArrays(data_types, settings, type_indexes, json_info);

            /// Convert Maps to Objects if needed.
            if (settings.json.allow_object_type)
                transformMapsAndObjectsToObjects(data_types, type_indexes);

            if (settings.json.read_objects_as_strings)
                transformMapsObjectsAndStringsToStrings(data_types, type_indexes);
        };

        transformTypesRecursively(types, transform_simple_types, transform_complex_types);
    }

    template <bool is_json>
    DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth = 1);

    bool tryInferDate(std::string_view field)
    {
        if (field.empty())
            return false;

        ReadBufferFromString buf(field);
        Float64 tmp_float;
        /// Check if it's just a number, and if so, don't try to infer Date from it,
        /// because we can interpret this number as a Date (for example 20000101 will be 2000-01-01)
        /// and it will lead to inferring Date instead of simple Int64/UInt64 in some cases.
        if (tryReadFloatText(tmp_float, buf) && buf.eof())
            return false;

        buf.seek(0, SEEK_SET); /// Return position to the beginning

        DayNum tmp;
        return tryReadDateText(tmp, buf) && buf.eof();
    }

    bool tryInferDateTime(std::string_view field, const FormatSettings & settings)
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

    template <bool is_json>
    DataTypePtr tryInferArray(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('[', buf);
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != ']')
        {
            if (!first)
            {
                /// Skip field delimiter between array elements.
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info, depth + 2);

            if (nested_type)
                nested_types.push_back(nested_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
        }

        /// No ']' at the end.
        if (buf.eof())
            return nullptr;

        assertChar(']', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type)
            return nullptr;

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

    DataTypePtr tryInferTuple(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('(', buf);
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != ')')
        {
            if (!first)
            {
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = tryInferDataTypeForSingleFieldImpl<false>(buf, settings, json_info, depth + 1);
            if (nested_type)
                nested_types.push_back(nested_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
        }

        /// No ')' at the end.
        if (buf.eof())
            return nullptr;

        assertChar(')', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type || nested_types.empty())
            return nullptr;

        return std::make_shared<DataTypeTuple>(nested_types);
    }

    DataTypePtr tryInferNumber(ReadBuffer & buf, const FormatSettings & settings)
    {
        if (buf.eof())
            return nullptr;

        Float64 tmp_float;
        if (settings.try_infer_integers)
        {
            /// If we read from String, we can do it in a more efficient way.
            if (auto * string_buf = dynamic_cast<ReadBufferFromString *>(&buf))
            {
                /// Remember the pointer to the start of the number to rollback to it.
                char * number_start = buf.position();
                Int64 tmp_int;
                bool read_int = tryReadIntText(tmp_int, buf);
                /// If we reached eof, it cannot be float (it requires no less data than integer)
                if (buf.eof())
                    return read_int ? std::make_shared<DataTypeInt64>() : nullptr;

                char * int_end = buf.position();
                /// We can safely get back to the start of the number, because we read from a string and we didn't reach eof.
                buf.position() = number_start;

                bool read_uint = false;
                char * uint_end = nullptr;
                /// In case of Int64 overflow we can try to infer UInt64.
                if (!read_int)
                {
                    UInt64 tmp_uint;
                    read_uint = tryReadIntText(tmp_uint, buf);
                    /// If we reached eof, it cannot be float (it requires no less data than integer)
                    if (buf.eof())
                        return read_uint ? std::make_shared<DataTypeUInt64>() : nullptr;

                    uint_end = buf.position();
                    buf.position() = number_start;
                }

                if (tryReadFloatText(tmp_float, buf))
                {
                    if (read_int && buf.position() == int_end)
                        return std::make_shared<DataTypeInt64>();
                    if (read_uint && buf.position() == uint_end)
                        return std::make_shared<DataTypeUInt64>();
                    return std::make_shared<DataTypeFloat64>();
                }

                return nullptr;
            }

            /// We should use PeekableReadBuffer, because we need to
            /// rollback to the start of number to parse it as integer first
            /// and then as float.
            PeekableReadBuffer peekable_buf(buf);
            PeekableReadBufferCheckpoint checkpoint(peekable_buf);
            Int64 tmp_int;
            bool read_int = tryReadIntText(tmp_int, peekable_buf);
            auto * int_end = peekable_buf.position();
            peekable_buf.rollbackToCheckpoint(true);

            bool read_uint = false;
            char * uint_end = nullptr;
            /// In case of Int64 overflow we can try to infer UInt64.
            if (!read_int)
            {
                PeekableReadBufferCheckpoint new_checkpoint(peekable_buf);
                UInt64 tmp_uint;
                read_uint = tryReadIntText(tmp_uint, peekable_buf);
                uint_end = peekable_buf.position();
                peekable_buf.rollbackToCheckpoint(true);
            }

            if (tryReadFloatText(tmp_float, peekable_buf))
            {
                /// Float parsing reads no fewer bytes than integer parsing,
                /// so position of the buffer is either the same, or further.
                /// If it's the same, then it's integer.
                if (read_int && peekable_buf.position() == int_end)
                    return std::make_shared<DataTypeInt64>();
                if (read_uint && peekable_buf.position() == uint_end)
                    return std::make_shared<DataTypeUInt64>();
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

    template <bool is_json>
    DataTypePtr tryInferString(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
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
                if (auto number_type = tryInferNumberFromString(field, settings))
                {
                    json_info->numbers_parsed_from_json_strings.insert(number_type.get());
                    return number_type;
                }
            }
        }

        return std::make_shared<DataTypeString>();
    }

    template <bool is_json>
    DataTypePtr tryInferMapOrObject(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('{', buf);
        skipWhitespaceIfAny(buf);

        DataTypes key_types;
        DataTypes value_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != '}')
        {
            if (!first)
            {
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
                key_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, nullptr, depth + 1);
            }

            if (key_type)
                key_types.push_back(key_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
            if (!checkChar(':', buf))
                return nullptr;
            skipWhitespaceIfAny(buf);

            auto value_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info, depth + 1);
            if (value_type)
                value_types.push_back(value_type);
            else
                have_invalid_nested_type = true;
            skipWhitespaceIfAny(buf);
        }

        /// No '}' at the end.
        if (buf.eof())
            return nullptr;

        assertChar('}', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type)
            return nullptr;

        if (key_types.empty())
        {
            if constexpr (is_json)
            {
                if (settings.json.allow_object_type)
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
                if (settings.json.allow_object_type)
                    return std::make_shared<DataTypeObject>("json", true);
                if (settings.json.read_objects_as_strings)
                    return std::make_shared<DataTypeString>();
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
    DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        if (depth > settings.max_parser_depth)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum parse depth ({}) exceeded. Consider rising max_parser_depth setting.", settings.max_parser_depth);

        skipWhitespaceIfAny(buf);

        if (buf.eof())
            return nullptr;

        /// Array [field1, field2, ...]
        if (*buf.position() == '[')
            return tryInferArray<is_json>(buf, settings, json_info, depth);

        /// Tuple (field1, field2, ...), if format is not JSON
        if constexpr (!is_json)
        {
            if (*buf.position() == '(')
                return tryInferTuple(buf, settings, json_info, depth);
        }

        /// Map/Object for JSON { key1 : value1, key2 : value2, ...}
        if (*buf.position() == '{')
            return tryInferMapOrObject<is_json>(buf, settings, json_info, depth);

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
}

void transformInferredTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<false>(types, settings, nullptr);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

void transformInferredJSONTypesIfNeeded(
    DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<true>(types, settings, json_info);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

void transformJSONTupleToArrayIfPossible(DataTypePtr & data_type, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    if (!data_type)
        return;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        auto nested_type = array_type->getNestedType();
        transformJSONTupleToArrayIfPossible(nested_type, settings, json_info);
        data_type = std::make_shared<DataTypeArray>(nested_type);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(data_type.get()))
    {
        auto value_type = map_type->getValueType();
        transformJSONTupleToArrayIfPossible(value_type, settings, json_info);
        data_type = std::make_shared<DataTypeMap>(map_type->getKeyType(), value_type);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(data_type.get()))
    {
        auto nested_types = tuple_type->getElements();
        for (auto & nested_type : nested_types)
            transformJSONTupleToArrayIfPossible(nested_type, settings, json_info);

        auto nested_types_copy = nested_types;
        transformInferredTypesIfNeededImpl<true>(nested_types_copy, settings, json_info);
        if (checkIfTypesAreEqual(nested_types_copy))
            data_type = std::make_shared<DataTypeArray>(nested_types_copy.back());
        else
            data_type = std::make_shared<DataTypeTuple>(nested_types);

        return;
    }
}

DataTypePtr tryInferNumberFromString(std::string_view field, const FormatSettings & settings)
{
    ReadBufferFromString buf(field);

    if (settings.try_infer_integers)
    {
        Int64 tmp_int;
        if (tryReadIntText(tmp_int, buf) && buf.eof())
            return std::make_shared<DataTypeInt64>();

        /// We can safely get back to the start of buffer, because we read from a string and we didn't reach eof.
        buf.position() = buf.buffer().begin();

        /// In case of Int64 overflow, try to infer UInt64
        UInt64 tmp_uint;
        if (tryReadIntText(tmp_uint, buf) && buf.eof())
            return std::make_shared<DataTypeUInt64>();
    }

    /// We can safely get back to the start of buffer, because we read from a string and we didn't reach eof.
    buf.position() = buf.buffer().begin();

    Float64 tmp;
    if (tryReadFloatText(tmp, buf) && buf.eof())
        return std::make_shared<DataTypeFloat64>();

    return nullptr;
}

DataTypePtr tryInferDateOrDateTimeFromString(std::string_view field, const FormatSettings & settings)
{
    if (settings.try_infer_dates && tryInferDate(field))
        return std::make_shared<DataTypeDate>();

    if (settings.try_infer_datetimes && tryInferDateTime(field, settings))
        return std::make_shared<DataTypeDateTime64>(9);

    return nullptr;
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

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(std::move(nested_types), tuple_type->getElementNames());

        return std::make_shared<DataTypeTuple>(std::move(nested_types));

    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursively(map_type->getKeyType());
        auto value_type = makeNullableRecursively(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCardinality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursively(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    if (which.isObject())
    {
        const auto * object_type = assert_cast<const DataTypeObject *>(type.get());
        if (object_type->hasNullableSubcolumns())
            return type;
        return std::make_shared<DataTypeObject>(object_type->getSchemaFormat(), true);
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
