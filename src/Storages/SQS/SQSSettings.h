#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Common/SettingsChanges.h>

namespace DB
{
class ASTStorage;
struct SQSSettingsImpl;

/// List of available types supported in SQSSettings object
#define SQS_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
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
    M(CLASS_NAME, Milliseconds) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, StreamingHandleErrorMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

SQS_SETTINGS_SUPPORTED_TYPES(SQSSettings, DECLARE_SETTING_TRAIT)

struct SQSSettings
{
    SQSSettings();
    SQSSettings(const SQSSettings & settings);
    SQSSettings(SQSSettings && settings) noexcept;
    ~SQSSettings();

    SQS_SETTINGS_SUPPORTED_TYPES(SQSSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);

    SettingsChanges getFormatSettings() const;

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<SQSSettingsImpl> impl;
};

}
