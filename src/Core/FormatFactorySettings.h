#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <base/types.h>

namespace DB
{
struct FormatFactorySettingsImpl;
struct SettingChange;
class SettingsChanges;

#define FORMAT_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, IdentifierQuotingStyle)

FORMAT_SETTINGS_SUPPORTED_TYPES(FormatFactorySettings, DECLARE_SETTING_TRAIT)

struct FormatFactorySettings
{
    FormatFactorySettings();
    ~FormatFactorySettings();

    FORMAT_SETTINGS_SUPPORTED_TYPES(FormatFactorySettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// General API as needed
    bool tryGet(std::string_view name, Field & value) const;
    Field get(std::string_view name) const;
    void set(std::string_view name, const Field & value);
    bool has(std::string_view name) const;
    void applyChange(const SettingChange & change);
    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<FormatFactorySettingsImpl> impl;
};

}
