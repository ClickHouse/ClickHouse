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
