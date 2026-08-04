#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace DB
{

struct QueryPlanSerializationSettingsImpl;

/// List of available types supported in QueryPlanSerializationSettings object.
/// Extend cautiously: types must have stable SettingField implementations and
/// well-defined binary serialization. Adding a type enables operator[] overloads.
#define QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, Double) \
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
 *  2. The step is asked to `serializeSettings(settings)` (step-specific method)
 *     so it copies only the settings it depends on (usually via context->getSettingsRef()).
 *  3. `writeChangedBinary()` writes out just the non-default values after the step header, preceding the step-specific payload.
 * Deserialize:
 *  1. A new settings instance is constructed per step right after reading the step's output header.
 *  2. readBinary() populates modifications relative to defaults.
 *  3. The Deserialization context handed to factory (QueryPlanStepRegistry) contains a const reference to these settings
 *     so the step constructor can adapt its behavior as originally serialized.
 */
struct QueryPlanSerializationSettings
{
    QueryPlanSerializationSettings();
    QueryPlanSerializationSettings(const QueryPlanSerializationSettings & settings);
    ~QueryPlanSerializationSettings();

    /// Serialize only settings that differ from defaults.
    void writeChangedBinary(WriteBuffer & out) const;
    /// Read settings updating only those present in the stream; missing ones keep defaults.
    void readBinary(ReadBuffer & in);

    /// Whether a setting with this name, or an alias of it, exists. Validation uses it to spot an
    /// unknown setting without trying to decode its value.
    static bool hasSetting(std::string_view name);

    /// A changed setting as it travels in the plan outline. The value has its length in front of
    /// it, so a reader can skip a setting it does not know when the writer marked it ignorable.
    /// Otherwise it has to refuse the plan: leaving an unknown setting at its default would change
    /// how the query runs without saying so.
    struct SerializedEntry
    {
        String name;
        UInt8 flags = 0;
        String value;   /// the setting-field binary encoding

        static constexpr UInt8 FLAG_IGNORABLE = 1;
    };

    /// Changed settings as outline entries.
    std::vector<SerializedEntry> getChangedEntries() const;

    /// The oldest plan version that can read this entry. Every setting and value written today is
    /// older than the outline, so this returns the base version. A setting or a new value of one
    /// added later has to raise it here, so plans carrying it are only sent to readers new enough,
    /// unless the entry is marked ignorable.
    static UInt64 minReaderVersionForEntry(const SerializedEntry & entry);
    /// Applies the entries. An unknown one is skipped if it is marked ignorable and refused if it
    /// is not, and a value that does not use up exactly its own bytes is refused.
    void applyEntries(const std::vector<SerializedEntry> & entries);

    /// Generated operator[] overloads for each supported type category.
    QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

private:
    std::unique_ptr<QueryPlanSerializationSettingsImpl> impl;
};

}
