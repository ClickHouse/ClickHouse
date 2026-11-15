#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace DB
{

struct QueryPlanSerializationSettingsImpl;

/// List of available types supported in QueryPlanSerializationSettings object.
/// Extend cautiously: types must have stable SettingField implementations and
/// a string representation that round-trips the value exactly. Adding a type enables operator[] overloads.
#define QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, HashJoinLayout) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, JoinAlgorithm) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, OverflowMode) \
    M(CLASS_NAME, OverflowModeGroupBy) \
    M(CLASS_NAME, Seconds) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, TotalsMode) \
    M(CLASS_NAME, UInt64) \

QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_TRAIT)

/*
 * A QueryPlanStep may depend on certain settings (limits, algorithm toggles, thresholds).
 * Each step can have its own values of the settings (e.g. via SETTINGS clause in different subqueries).
 * Persisting the full Settings object would be excessive.
 * This lightweight container stores only the subset considered relevant for plan step reconstruction.
 *
 * Usage lifecycle within QueryPlan (de)serialization:
 * Serialize:
 *  1. For every node/step in depth-first traversal QueryPlan::serialize creates a QueryPlanSerializationSettings instance.
 *  2. The step is asked to `serializeSettings(settings, version)` (step-specific method)
 *     so it copies only the settings it depends on (usually via context->getSettingsRef()).
 *     `version` is the query-plan serialization version the stream is being written with
 *     (already the minimum of ours and the peer's), so a step can withhold a setting name
 *     the receiving peer would reject as unknown.
 *  3. `write` writes out just the changed values after the step header, preceding the step-specific payload.
 *     Each value is written by name, as a string, with flags, like `Settings` in the native protocol.
 * Deserialize:
 *  1. A new settings instance is constructed per step right after reading the step's output header.
 *  2. `read` populates modifications relative to defaults. A name the reader does not know is skipped with
 *     a warning, unless the writer declared the setting with the `IMPORTANT` flag; then the read throws.
 *     So a new setting that only tunes performance can be added without gating it on the plan version,
 *     and a setting that changes the result or the data layout must be declared `IMPORTANT`.
 *  3. The Deserialization context handed to factory (QueryPlanStepRegistry) contains a const reference to these settings
 *     so the step constructor can adapt its behavior as originally serialized.
 */
struct QueryPlanSerializationSettings
{
    QueryPlanSerializationSettings();
    QueryPlanSerializationSettings(const QueryPlanSerializationSettings & settings);
    ~QueryPlanSerializationSettings();

    /// Serialize only changed settings: by name, with the value as a string.
    void write(WriteBuffer & out) const;
    /// Read settings updating only those present in the stream; missing ones keep defaults.
    void read(ReadBuffer & in);

    /// Generated operator[] overloads for each supported type category.
    QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

private:
    std::unique_ptr<QueryPlanSerializationSettingsImpl> impl;
};

}
