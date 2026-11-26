#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/findExtreme.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Settings.h>
#include <DataTypes/TimezoneMixin.h>
#include <DataTypes/TypeTree.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionResultSetting.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>

#include <limits>


namespace DB
{

namespace Setting
{
    extern const SettingsAggregateFunctionInputFormat aggregate_function_input_format;
    extern const SettingsString bool_false_representation;
    extern const SettingsString bool_true_representation;
    extern const SettingsDateTimeInputFormat cast_string_to_date_time_mode;
    extern const SettingsBool date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands;
    extern const SettingsDateTimeInputFormat date_time_input_format;
    extern const SettingsDateTimeOutputFormat date_time_output_format;
    extern const SettingsBool input_format_ipv4_default_on_conversion_error;
    extern const SettingsBool input_format_ipv6_default_on_conversion_error;
    extern const SettingsBool input_format_read_datetime_number_as_raw_value;
    extern const SettingsBool input_format_tsv_enum_as_number;
    extern const SettingsIntervalOutputFormat interval_output_format;
    extern const SettingsBool json_type_escape_dots_in_keys;
    extern const SettingsBool output_format_always_write_decimal_point_in_float_and_decimal;
    extern const SettingsBool output_format_decimal_trailing_zeros;
    extern const SettingsUInt64 output_format_float_precision;
    extern const SettingsBool output_format_trim_fixed_string;
    extern const SettingsBool output_format_values_escape_quote_with_quote;
    extern const SettingsBool precise_float_parsing;
    extern const SettingsBool type_json_skip_invalid_typed_paths;
    extern const SettingsBool type_json_skip_null_typed_paths;
    extern const SettingsBool type_json_use_partial_match_to_skip_paths_by_regexp;
    extern const SettingsBool cast_string_to_dynamic_use_inference;
    extern const SettingsBool cast_string_to_variant_use_inference;
    extern const SettingsBool input_format_json_try_infer_numbers_from_strings;
    extern const SettingsBool input_format_try_infer_dates;
    extern const SettingsBool input_format_try_infer_datetimes;
    extern const SettingsBool input_format_try_infer_datetimes_only_datetime64;
    extern const SettingsBool input_format_try_infer_exponent_floats;
    extern const SettingsBool input_format_try_infer_integers;
    extern const SettingsBool input_format_try_infer_variants;
    extern const SettingsBool input_format_tsv_use_best_effort_in_schema_inference;
    extern const SettingsUInt64Auto schema_inference_make_columns_nullable;
    extern const SettingsTimezone session_timezone;
    JSON_WRITING_RESULT_SETTINGS(DECLARE_JSON_WRITING_RESULT_SETTING)
    extern const SettingsBool decimal_check_overflow;
    extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
}

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int SIZES_OF_ARRAYS_DONT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!isColumnConst(*column))
        return {};

    const ColumnConst * res = assert_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn()) || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


Columns convertConstTupleToConstantElements(const ColumnConst & column)
{
    const ColumnTuple & src_tuple = assert_cast<const ColumnTuple &>(column.getDataColumn());
    const auto & src_tuple_columns = src_tuple.getColumns();
    size_t tuple_size = src_tuple_columns.size();
    size_t rows = column.size();

    Columns res(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        res[i] = ColumnConst::create(src_tuple_columns[i], rows);

    return res;
}

ColumnWithTypeAndName columnGetNested(const ColumnWithTypeAndName & col)
{
    if (col.type->isNullable())
    {
        const DataTypePtr & nested_type = static_cast<const DataTypeNullable &>(*col.type).getNestedType();

        if (!col.column)
        {
            return ColumnWithTypeAndName{nullptr, nested_type, col.name};
        }
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*col.column))
        {
            const auto & nested_col = nullable->getNestedColumnPtr();
            return ColumnWithTypeAndName{nested_col, nested_type, col.name};
        }
        if (const auto * const_column = checkAndGetColumn<ColumnConst>(&*col.column))
        {
            const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&const_column->getDataColumn());

            ColumnPtr nullable_res;
            if (nullable_column)
            {
                const auto & nested_col = nullable_column->getNestedColumnPtr();
                nullable_res = ColumnConst::create(nested_col, col.column->size());
            }
            else
            {
                nullable_res = makeNullable(col.column);
            }
            return ColumnWithTypeAndName{nullable_res, nested_type, col.name};
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} for DataTypeNullable", col.dumpStructure());
    }
    return col;
}

ColumnsWithTypeAndName createBlockWithNestedColumns(const ColumnsWithTypeAndName & columns)
{
    ColumnsWithTypeAndName res;
    for (const auto & col : columns)
        res.emplace_back(columnGetNested(col));

    return res;
}

namespace
{

String withOrdinalEnding(size_t argument_index)
{
    /// `argument_index` is a zero-based argument index; produce the ordinal for the human-facing position `n = argument_index + 1`
    /// (1st, 2nd, 3rd, 4th, ...). The teens 11, 12, 13 use "th" despite ending in 1, 2, 3.
    const size_t n = argument_index + 1;
    const char * suffix = "th";
    if (n % 100 < 11 || n % 100 > 13)
    {
        switch (n % 10)
        {
            case 1:
                suffix = "st";
                break;
            case 2:
                suffix = "nd";
                break;
            case 3:
                suffix = "rd";
                break;
            default:
                break;
        }
    }
    return std::to_string(n) + suffix;
}

void validateArgumentsImpl(
    const String & function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t argument_offset,
    const FunctionArgumentDescriptors & descriptors)
{
    for (size_t i = 0; i < descriptors.size(); ++i)
    {
        const auto argument_index = i + argument_offset;
        if (argument_index >= arguments.size())
            break;

        const auto & arg = arguments[argument_index];
        const auto & descriptor = descriptors[i];
        if (int error_code = descriptor.isValid(arg.type, arg.column); error_code != 0)
            throw Exception(
                error_code,
                "A value of illegal type was provided as {} argument '{}' to function '{}'. Expected: {}, got: {}",
                withOrdinalEnding(argument_index),
                descriptor.name,
                function_name,
                descriptor.type_name,
                arg.type ? arg.type->getName() : "<?>");
    }
}

void validateVariadicArgumentsImpl(
    const String & function_name,
    const ColumnsWithTypeAndName & arguments,
    size_t argument_offset,
    const FunctionArgumentDescriptor & variadic_descriptor)
{
    for (size_t argument_index = argument_offset; argument_index < arguments.size(); ++argument_index)
    {
        const auto & arg = arguments[argument_index];
        if (int error_code = variadic_descriptor.isValid(arg.type, arg.column); error_code != 0)
            throw Exception(
                error_code,
                "A value of illegal type was provided as {} argument '{}' to function '{}'. Expected: {}, got: {}",
                withOrdinalEnding(argument_index),
                variadic_descriptor.name,
                function_name,
                variadic_descriptor.type_name,
                arg.type ? arg.type->getName() : "<?>");
    }
}

}

int FunctionArgumentDescriptor::isValid(const DataTypePtr & data_type, const ColumnPtr & column) const
{
    if (name.empty() || type_name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "name or type_name are not set");

    if (type_validator && (data_type == nullptr || !type_validator(*data_type)))
        return ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT;

    if (column_validator && (column == nullptr || !column_validator(*column)))
        return ErrorCodes::ILLEGAL_COLUMN;

    return 0;
}

void validateFunctionArguments(
    const IFunction & func,
    const ColumnsWithTypeAndName & arguments,
    const FunctionArgumentDescriptors & mandatory_args,
    const FunctionArgumentDescriptors & optional_args)
{
    validateFunctionArguments(func.getName(), arguments, mandatory_args, optional_args);
}

std::string_view getArgumentSingularOrPlural(const auto & args)
{
    return args.size() == 1 ? "argument" : "arguments";
}

void validateFunctionArguments(
    const String & function_name,
    const ColumnsWithTypeAndName & arguments,
    const FunctionArgumentDescriptors & mandatory_args,
    const FunctionArgumentDescriptors & optional_args)
{
    if (arguments.size() < mandatory_args.size() || arguments.size() > mandatory_args.size() + optional_args.size())
    {
        String expected_args_string;
        if (!mandatory_args.empty() && !optional_args.empty())
            expected_args_string = fmt::format(
                "{} mandatory {} and {} optional {}",
                mandatory_args.size(),
                getArgumentSingularOrPlural(mandatory_args),
                optional_args.size(),
                getArgumentSingularOrPlural(optional_args));
        else if (!mandatory_args.empty() && optional_args.empty())
            expected_args_string = fmt::format(
                "{} {}", mandatory_args.size(), getArgumentSingularOrPlural(mandatory_args)); /// intentionally not "_mandatory_ arguments"
        else if (mandatory_args.empty() && !optional_args.empty())
            expected_args_string = fmt::format("{} optional {}", optional_args.size(), getArgumentSingularOrPlural(optional_args));
        else
            expected_args_string = "0 arguments";

        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "An incorrect number of arguments was specified for function '{}'. Expected {}, got {}",
            function_name,
            expected_args_string,
            fmt::format("{} {}", arguments.size(), getArgumentSingularOrPlural(arguments)));
    }

    validateArgumentsImpl(function_name, arguments, 0, mandatory_args);
    if (!optional_args.empty())
        validateArgumentsImpl(function_name, arguments, mandatory_args.size(), optional_args);
}

void validateFunctionArgumentsWithVariadics(
    const IFunction & func,
    const ColumnsWithTypeAndName & arguments,
    const FunctionArgumentDescriptors & mandatory_args,
    const FunctionArgumentDescriptor & variadic_args)
{
    if (arguments.size() < mandatory_args.size())
    {
        String expected_args_string;
        if (!mandatory_args.empty())
            expected_args_string = fmt::format(
                "{} {}",
                mandatory_args.size(),
                getArgumentSingularOrPlural(mandatory_args));
        else if (!mandatory_args.empty())
            expected_args_string = fmt::format(
                "{} {}", mandatory_args.size(), getArgumentSingularOrPlural(mandatory_args));
        else
            expected_args_string = "0 arguments";

        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "An incorrect number of arguments was specified for function '{}'. Expected at least {}, got {}",
            func.getName(),
            expected_args_string,
            fmt::format("{} {}", arguments.size(), getArgumentSingularOrPlural(arguments)));
    }

    validateArgumentsImpl(func.getName(), arguments, 0, mandatory_args);
    validateVariadicArgumentsImpl(func.getName(), arguments, mandatory_args.size(), variadic_args);
}

void validateNumberOfFunctionArguments(
    const IFunction & func,
    const ColumnsWithTypeAndName & arguments,
    size_t expected_min_args,
    size_t expected_max_args)
{
    if (arguments.size() < expected_min_args || arguments.size() > expected_max_args)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for function {}. Expected {} or {}, got {}",
            func.getName(),
            expected_min_args,
            expected_max_args,
            arguments.size());
        }
}

std::pair<VectorWithMemoryTracking<const IColumn *>, const ColumnArray::Offset *>
checkAndGetNestedArrayOffset(const IColumn ** columns, size_t num_arguments)
{
    chassert(num_arguments > 0);
    VectorWithMemoryTracking<const IColumn *> nested_columns(num_arguments);
    const ColumnArray::Offsets * offsets = nullptr;
    for (size_t i = 0; i < num_arguments; ++i)
    {
        const ColumnArray::Offsets * offsets_i = nullptr;
        if (const ColumnArray * arr = checkAndGetColumn<const ColumnArray>(columns[i]))
        {
            nested_columns[i] = &arr->getData();
            offsets_i = &arr->getOffsets();
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} as argument of function", columns[i]->getName());
        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Lengths of all arrays passed to aggregate function must be equal.");
    }
    return {nested_columns, offsets->data()};  /// NOLINT(clang-analyzer-core.CallAndMessage)
}

ColumnPtr
wrapInNullable(const ColumnPtr & src, const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count)
{
    ColumnPtr result_null_map_column;

    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;

    if (src->onlyNull())
        return src;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*src))
    {
        src_not_nullable = nullable->getNestedColumnPtr();
        result_null_map_column = nullable->getNullMapColumnPtr();
    }

    for (const auto & elem : args)
    {
        if (!elem.type->isNullable())
            continue;

        /// Const Nullable that are NULL.
        if (elem.column->onlyNull())
        {
            chassert(result_type->isNullable());
            return result_type->createColumnConstWithDefaultValue(input_rows_count);
        }

        if (isColumnConst(*elem.column))
            continue;

        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*elem.column))
        {
            const ColumnPtr & null_map_column = nullable->getNullMapColumnPtr();
            if (!result_null_map_column)
            {
                result_null_map_column = null_map_column;
            }
            else
            {
                MutableColumnPtr mutable_result_null_map_column = IColumn::mutate(std::move(result_null_map_column));

                NullMap & result_null_map = assert_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
                const NullMap & src_null_map = assert_cast<const ColumnUInt8 &>(*null_map_column).getData();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    result_null_map[i] |= src_null_map[i];

                result_null_map_column = std::move(mutable_result_null_map_column);
            }
        }
    }

    if (!result_null_map_column)
        return makeNullable(src);

    return ColumnNullable::create(src_not_nullable->convertToFullColumnIfConst(), result_null_map_column);
}

ColumnPtr wrapInNullable(const ColumnPtr & src, ColumnPtr null_map)
{
    if (src->onlyNull())
        return src;

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(src.get()))
    {
        ColumnPtr src_not_nullable = nullable->getNestedColumnPtr();
        const auto & src_null_map = nullable->getNullMapColumn().getData();

        if (null_map)
        {
            /// Reuse null_map as result_null_map if possible, thus avoiding unnecessary memory allocation.
            auto result_null_map_column = IColumn::mutate(std::move(null_map));
            auto & result_null_map = assert_cast<ColumnUInt8 &>(*result_null_map_column).getData();
            for (size_t i = 0; i < result_null_map.size(); ++i)
                result_null_map[i] |= src_null_map[i];
            null_map = std::move(result_null_map_column);
        }
        else
        {
            /// Share the null map between src and result.
            null_map = nullable->getNullMapColumnPtr();
        }

        return ColumnNullable::create(src_not_nullable->convertToFullColumnIfConst(), null_map);
    }
    if (const auto * const_src = checkAndGetColumn<ColumnConst>(src.get()))
    {
        /// Only collapse the result to a single ColumnConst when the input null map is
        /// uniform (or absent). Per-row null maps cannot be folded onto a constant value
        /// without losing the not-null rows.
        bool uniform_null_map = true;
        UInt8 is_null = 0;
        if (null_map)
        {
            const NullMap & null_map_data = assert_cast<const ColumnUInt8 &>(*null_map).getData();
            is_null = null_map_data.empty() ? 0 : null_map_data[0];
            for (size_t i = 1, size = null_map_data.size(); i < size; ++i)
            {
                if (null_map_data[i] != is_null)
                {
                    uniform_null_map = false;
                    break;
                }
            }
        }

        if (uniform_null_map)
        {
            ColumnPtr result_null_map_column = ColumnUInt8::create(1, is_null || const_src->isNullAt(0));
            const auto * nullable_data = checkAndGetColumn<ColumnNullable>(&const_src->getDataColumn());
            auto data_not_nullable = nullable_data ? nullable_data->getNestedColumnPtr() : const_src->getDataColumnPtr();
            return ColumnConst::create(ColumnNullable::create(data_not_nullable, result_null_map_column), const_src->size());
        }

        /// Heterogeneous null map: expand the constant and merge the inner null map (if any)
        /// into the caller's per-row null_map.
        const auto * nullable_data = checkAndGetColumn<ColumnNullable>(&const_src->getDataColumn());
        if (nullable_data && nullable_data->isNullAt(0))
        {
            auto result_null_map_column = IColumn::mutate(std::move(null_map));
            auto & result_null_map = assert_cast<ColumnUInt8 &>(*result_null_map_column).getData();
            std::fill(result_null_map.begin(), result_null_map.end(), UInt8(1));
            null_map = std::move(result_null_map_column);
        }
        ColumnPtr inner = nullable_data
            ? ColumnConst::create(nullable_data->getNestedColumnPtr(), const_src->size())
            : src;
        return ColumnNullable::create(inner->convertToFullColumnIfConst(), null_map);
    }
    /// Past the `Nullable` and `Const` branches, `src` is guaranteed to be neither, so
    /// `convertToFullColumnIfConst` would be a no-op that only adds a ref to the underlying
    /// column. Pass `src` directly so that `ColumnNullable::create(const ColumnPtr &, ...)`
    /// sees `use_count() == 1` when the caller is the unique owner.
    if (null_map)
        return ColumnNullable::create(src, null_map);
    return ColumnNullable::create(src, ColumnUInt8::create(src->size(), UInt8(0)));
}

NullPresence getNullPresense(const ColumnsWithTypeAndName & args)
{
    NullPresence res;

    for (const auto & elem : args)
    {
        res.has_nullable |= elem.type->isNullable();
        res.has_null_constant |= elem.type->onlyNull();
    }

    return res;
}

bool conversionResultDependsOnSetting(
    const FunctionResultSetting & setting, const DataTypePtr & from, const DataTypePtr & to, bool creates_result_type)
{
    /// The settings of inferring the type of a value from its text, used only for a `Dynamic`, `Variant` or `JSON` value.
    static const FunctionResultSetting inference_settings[] = {
        Setting::cast_string_to_dynamic_use_inference, Setting::cast_string_to_variant_use_inference,
        Setting::input_format_json_try_infer_numbers_from_strings, Setting::input_format_try_infer_dates,
        Setting::input_format_try_infer_datetimes, Setting::input_format_try_infer_datetimes_only_datetime64,
        Setting::input_format_try_infer_exponent_floats, Setting::input_format_try_infer_integers,
        Setting::input_format_try_infer_variants, Setting::input_format_tsv_use_best_effort_in_schema_inference,
        Setting::schema_inference_make_columns_nullable};

    /// The settings of parsing text.
    static const FunctionResultSetting parsing_settings[] = {
        Setting::date_time_input_format, Setting::cast_string_to_date_time_mode, Setting::precise_float_parsing,
        Setting::input_format_ipv4_default_on_conversion_error, Setting::input_format_ipv6_default_on_conversion_error,
        Setting::input_format_tsv_enum_as_number, Setting::input_format_read_datetime_number_as_raw_value,
        Setting::aggregate_function_input_format};

    /// The settings of writing a value as text.
    static const FunctionResultSetting writing_settings[] = {
        Setting::date_time_output_format, Setting::date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands,
        Setting::interval_output_format, Setting::output_format_decimal_trailing_zeros,
        Setting::output_format_always_write_decimal_point_in_float_and_decimal,
        Setting::output_format_float_precision, Setting::output_format_trim_fixed_string,
        Setting::output_format_values_escape_quote_with_quote};

    /// The spellings of `Bool` values, used both to write and to parse them.
    static const FunctionResultSetting bool_settings[] = {Setting::bool_true_representation, Setting::bool_false_representation};

    /// The settings of writing a `JSON` value as text.
    static const FunctionResultSetting json_writing_settings[] = {JSON_WRITING_RESULT_SETTINGS(LIST_JSON_WRITING_RESULT_SETTING)};

    /// The settings of the `JSON` type.
    static const FunctionResultSetting json_settings[] = {
        Setting::type_json_skip_null_typed_paths, Setting::json_type_escape_dots_in_keys, Setting::type_json_skip_invalid_typed_paths,
        Setting::type_json_use_partial_match_to_skip_paths_by_regexp};

    auto has_type = [](const DataTypePtr & type, auto && predicate)
    {
        bool found = false;
        forEachInTypeTree(*type, [&](const IDataType & element) { found = found || predicate(element); });
        return found;
    };
    auto is_text = [](const IDataType & type) { return isStringOrFixedString(type); };
    /// A `Dynamic`, `Variant` or `JSON` value can hold a value of any type at runtime.
    auto may_hold_any = [](const IDataType & type) { return isDynamic(type) || isVariant(type) || isObject(type); };

    if (std::ranges::contains(inference_settings, setting))
        return has_type(to, [](const IDataType & type) { return isDynamic(type) || isVariant(type) || isObject(type); });
    if (std::ranges::contains(parsing_settings, setting))
        return has_type(from, [&](const IDataType & type) { return is_text(type) || may_hold_any(type); });
    if (std::ranges::contains(writing_settings, setting))
        return has_type(to, is_text);
    if (std::ranges::contains(bool_settings, setting))
        return has_type(from, [&](const IDataType & type) { return is_text(type) || may_hold_any(type); }) || has_type(to, is_text);
    if (std::ranges::contains(json_writing_settings, setting))
        return has_type(from, may_hold_any) && has_type(to, is_text);
    if (std::ranges::contains(json_settings, setting))
        return has_type(from, may_hold_any) || has_type(to, may_hold_any);

    if (!(setting == FunctionResultSetting(Setting::session_timezone)))
        return true;

    auto has_implicit_time_zone = [](const IDataType & type)
    {
        const auto * time_zone = dynamic_cast<const TimezoneMixin *>(&type);
        return time_zone && !time_zone->hasExplicitTimeZone();
    };

    /// A time without an explicit time zone is written as text in the time zone of the session, e.g. `toString([dt])`, also when
    /// a `Dynamic` value holds it.
    if ((has_type(from, has_implicit_time_zone) || has_type(from, may_hold_any)) && has_type(to, is_text))
        return true;

    /// A type inferred from text can be a `DateTime` in the time zone of the session, e.g. a path of `CAST(s AS JSON)`.
    if (has_type(to, may_hold_any) && has_type(from, [&](const IDataType & type) { return is_text(type) || may_hold_any(type); }))
        return true;

    if (!has_type(to, has_implicit_time_zone))
        return false;

    /// A type without an explicit time zone that the function creates takes the time zone of the session, which changes how its
    /// values are written as text later, e.g. `toString(toDateTime(n))`.
    if (creates_result_type)
        return true;

    /// Text, and values without a time zone of their own, are placed in the time zone of the result.
    return has_type(from, [&](const IDataType & type)
        { return is_text(type) || may_hold_any(type) || isDateOrDate32(type) || isTime(type) || isTime64(type); });
}

bool arithmeticResultDependsOnSetting(const FunctionResultSetting & setting, const DataTypes & arguments, const DataTypePtr & result_type)
{
    auto has_type = [&](const std::function<bool(const IDataType &)> & predicate)
    {
        return anyInTypeTree(*result_type, predicate)
            || std::ranges::any_of(arguments, [&](const DataTypePtr & type) { return anyInTypeTree(*type, predicate); });
    };

    if (setting == FunctionResultSetting(Setting::decimal_check_overflow))
        return has_type([](const IDataType & type) { return isDecimal(type); });
    if (setting == FunctionResultSetting(Setting::date_time_overflow_behavior))
        return has_type([](const IDataType & type) { return isDateOrDate32OrDateTimeOrDateTime64(type) || isTime(type) || isTime64(type); });
    return true;
}

bool isDecimalOrNullableDecimal(const DataTypePtr & type)
{
    WhichDataType which(type);
    if (which.isDecimal())
        return true;
    if (!which.isNullable())
        return false;
    return isDecimal(assert_cast<const DataTypeNullable *>(type.get())->getNestedType());
}

bool isLowCardinalityType(const IDataType & type)
{
    return typeid_cast<const DataTypeLowCardinality *>(&type) != nullptr;
}

bool hasLowCardinalityTypes(const ColumnsWithTypeAndName & args)
{
    for (const auto & column : args)
    {
        /// recursiveRemoveLowCardinality returns the very same type object when nothing was removed.
        if (column.type && recursiveRemoveLowCardinality(column.type).get() != column.type.get())
            return true;
    }
    return false;
}

bool allArgumentColumnsAreConstant(const ColumnsWithTypeAndName & args)
{
    for (const auto & column : args)
    {
        if (!column.column || !isColumnConst(*column.column))
            return false;
    }
    return true;
}

bool plusMinusWithConstantsIsInjective(
    const ColumnWithTypeAndName & left, const ColumnWithTypeAndName & right, const DataTypePtr & return_type)
{
    /// Two varying operands are not injective (`x + y` maps many pairs to one sum), and with both
    /// fixed there is no varying argument to be injective in.
    const bool left_is_const = left.column && isColumnConst(*left.column);
    const bool right_is_const = right.column && isColumnConst(*right.column);
    if (left_is_const == right_is_const)
        return false;

    auto is_integer_type = [](const DataTypePtr & type)
    { return type && isInteger(*removeNullable(recursiveRemoveLowCardinality(type))); };

    if (!is_integer_type(left.type) || !is_integer_type(right.type) || !is_integer_type(return_type))
        return false;

    /// A NULL among the varying argument's values maps to NULL one-to-one, so only the fixed operand
    /// matters. Its `ColumnConst` nests a column of size 1, so the value is readable even when the
    /// constant itself was materialized with size 0, as query-plan constants are.
    const ColumnWithTypeAndName & constant = left_is_const ? left : right;
    return !constant.column->onlyNull();
}

bool convertLowCardinalityColumnsToFull(ColumnsWithTypeAndName & args)
{
    bool converted = false;
    for (auto & column : args)
    {
        auto column_without_low_cardinality = recursiveRemoveLowCardinality(column.column);
        auto type_without_low_cardinality = recursiveRemoveLowCardinality(column.type);

        converted |= type_without_low_cardinality.get() != column.type.get();

        column.column = std::move(column_without_low_cardinality);
        column.type = std::move(type_without_low_cardinality);
    }
    return converted;
}

/// Note that, for historical reasons, most of the functions use the first argument size to determine which is the
/// size of all the columns. When short circuit optimization was introduced, `input_rows_count` was also added for
/// all functions, but many have not been adjusted
void checkFunctionArgumentSizes(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
{
    for (size_t i = 0; i < arguments.size(); i++)
    {
        if (isColumnConst(*arguments[i].column))
            continue;

        size_t current_size = arguments[i].column->size();

        if (current_size != input_rows_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected the argument №{} ('{}' of type {}) to have {} rows, but it has {}",
                i + 1,
                arguments[i].name,
                arguments[i].type->getName(),
                input_rows_count,
                current_size);
    }
}

ColumnWithTypeAndName castNullableEnumToResultType(const ColumnWithTypeAndName & argument, const DataTypePtr & result_type)
{
    const auto target_type = makeNullableSafe(removeLowCardinality(result_type));
    if (!argument.type->isNullable() || !target_type->isNullable() || removeNullable(argument.type)->isDefaultInsertTrivial())
        return argument;

    return {castColumn(argument, target_type), target_type, argument.name};
}

void checkIntegerArgumentFitsInt64(const IColumn & column, std::string_view function_name, std::string_view argument_name)
{
    const IColumn * values = &column;
    if (const auto * column_const = typeid_cast<const ColumnConst *>(values))
        values = &column_const->getDataColumn();

    /// Only UInt64 can exceed Int64, which `IColumn::getInt` returns: 2^63 or more would be read as a negative number
    const auto * column_uint64 = typeid_cast<const ColumnVector<UInt64> *>(values);
    if (!column_uint64)
        return;

    const auto & data = column_uint64->getData();
    const auto max_value = findExtremeMax(data.data(), 0, data.size());
    if (max_value && *max_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        throw Exception(
            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
            "The {} argument of function {} is out of range for Int64: {}",
            argument_name,
            function_name,
            *max_value);
}

bool uint64ArgumentMayHoldUnusedValues(const ColumnsWithTypeAndName & arguments)
{
    bool has_uint64 = false;
    bool has_nullable = false;
    for (const auto & argument : arguments)
    {
        if (WhichDataType(removeNullable(removeLowCardinality(argument.type))).isUInt64())
        {
            /// The dictionary of a `LowCardinality` column may keep values that no row references, e.g. after a filter.
            if (argument.type->lowCardinality())
                return true;
            has_uint64 = true;
        }
        has_nullable |= isNullableOrLowCardinalityNullable(argument.type);
    }
    return has_uint64 && has_nullable;
}
}
