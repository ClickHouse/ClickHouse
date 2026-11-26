#pragma once

#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/SettingIndex.h>
#include <Common/VectorWithMemoryTracking.h>

#include <span>

namespace DB
{

struct Settings;

/// A setting that changes the result of a function for the same arguments, e.g. `formatdatetime_parsedatetime_m_is_month_name`
/// for `formatDateTime`. A function lists them in `static constexpr FunctionResultSetting result_settings[]`, or passes them to
/// `FunctionFactory::registerFunction`. Built from `Setting::...`, so a setting that does not exist or belongs to another settings
/// class does not compile.
struct FunctionResultSetting
{
    template <typename FieldType>
    constexpr FunctionResultSetting(const SettingIndex<Settings, FieldType> & setting) /// NOLINT(google-explicit-constructor)
        : offset(&setting.offset)
    {
    }

    bool operator==(const FunctionResultSetting & other) const { return offset == other.offset; }

    /// Points to the offset in `setting`: the offset itself is not known at compile time outside the definition of the setting.
    const size_t * offset;
};

using FunctionResultSettings = std::span<const FunctionResultSetting>;

/// The result settings of a function as `FunctionFactory` registered them. Lives as long as the factory.
struct RegisteredResultSettings
{
    VectorWithMemoryTracking<FunctionResultSetting> settings;
    Names names;
    /// For `Settings::getByIndex`: creating a function must not look up the names.
    VectorWithMemoryTracking<size_t> indices;
};

/// The values of the settings of `registered` when the function was created, in the same order.
struct CapturedResultSettings
{
    const RegisteredResultSettings * registered = nullptr;
    VectorWithMemoryTracking<Field> values;
};

}

/// The settings of writing a `JSON` value as text, all `SettingsBool`. See `conversionResultDependsOnSetting`.
#define JSON_WRITING_RESULT_SETTINGS(M) \
    M(output_format_json_quote_64bit_integers) \
    M(output_format_json_quote_64bit_floats) \
    M(output_format_json_quote_denormals) \
    M(output_format_json_quote_decimals) \
    M(output_format_json_escape_forward_slashes) \
    M(output_format_json_named_tuples_as_objects) \
    M(output_format_json_skip_null_value_in_named_tuples) \
    M(output_format_json_validate_utf8) \
    M(output_format_json_map_as_array_of_tuples)

#define DECLARE_JSON_WRITING_RESULT_SETTING(NAME) extern const SettingsBool NAME;
#define LIST_JSON_WRITING_RESULT_SETTING(NAME) Setting::NAME,
