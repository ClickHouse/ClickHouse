#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace DB
{

struct QueryPlanSerializationSettingsImpl;

/// List of available types supported in QueryPlanSerializationSettings object
#define QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, OverflowMode) \
    M(CLASS_NAME, OverflowModeGroupBy) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, TotalsMode) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, JoinAlgorithm) \

QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_TRAIT)

struct QueryPlanSerializationSettings
{
    QueryPlanSerializationSettings();
    QueryPlanSerializationSettings(const QueryPlanSerializationSettings & settings);
    ~QueryPlanSerializationSettings();

    void writeChangedBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

    QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

private:
    std::unique_ptr<QueryPlanSerializationSettingsImpl> impl;
};

}
