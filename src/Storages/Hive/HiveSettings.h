#pragma once

#include "config.h"

#if USE_HIVE

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
class ASTStorage;
struct HiveSettingsImpl;

/// List of available types supported in HiveSettings object
#define HIVE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

HIVE_SETTINGS_SUPPORTED_TYPES(HiveSettings, DECLARE_SETTING_TRAIT)

/** Settings for the Hive engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct HiveSettings
{
    HiveSettings();
    HiveSettings(const HiveSettings & settings);
    HiveSettings(HiveSettings && settings) noexcept;
    ~HiveSettings();

    HIVE_SETTINGS_SUPPORTED_TYPES(HiveSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void loadFromQuery(ASTStorage & storage_def);

private:
    std::unique_ptr<HiveSettingsImpl> impl;
};
}

#endif
