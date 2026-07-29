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
    /// `version` is the negotiated query plan serialization version of the receiver: settings that
    /// were introduced after that version are omitted, because a receiver does not know their names
    /// and `readBinary` throws on unknown setting names.
    void writeChangedBinary(WriteBuffer & out, UInt64 version) const;
    /// Read settings updating only those present in the stream; missing ones keep defaults.
    void readBinary(ReadBuffer & in);

    /// Whether the setting was explicitly assigned or was present in the deserialized stream
    /// (readBinary marks only the settings it reads as changed). Lets a step's deserialization
    /// distinguish a value omitted by an older sender from one sent equal to the default.
    bool isChanged(std::string_view name) const;

    /// The minimum query plan serialization version required to serialize these settings without
    /// changing the receiver's behavior, i.e. the version below which `writeChangedBinary` would
    /// omit a version-gated setting whose value actually matters. Returns the baseline version 1
    /// when omitting the version-gated settings degrades gracefully (the receiver then behaves like
    /// an older server); in particular, a version-4 setting merely being marked changed does not
    /// raise the version, because steps mark every serialized setting changed even at its default.
    UInt64 getMinRequiredVersion() const;

    /// Whether the `max_memory_usage` value assigned by a join step differs from the query-wide
    /// setting (a subquery-local SETTINGS override). Not a serialized setting - the receiver
    /// recomputes it against its query context - it only feeds getMinRequiredVersion: an omitted
    /// step-local value cannot be restored on the receiver, so the stream must carry it.
    bool max_memory_usage_is_step_local = false;

    /// Whether the serializing join step's kind can resolve to an implementation that consults
    /// `enable_join_in_memory_compression`. CROSS (and COMMA, always executed as CROSS) join keeps
    /// its own dedicated threshold-based compression path and PASTE join stores no build side, so
    /// their fragments must not be raised to the version carrying the setting - a pre-version-4
    /// receiver would reject them during a rolling upgrade for a setting they never consume. Not a
    /// serialized setting; it only feeds getMinRequiredVersion. Defaults to true so a step that
    /// does not know its join kind keeps the conservative version bump.
    bool join_kind_consumes_in_memory_compression = true;

    /// Whether the serializing join step is executed by `ConstantJoin` (an explicit CROSS/COMMA join,
    /// or a join with a constant predicate like `ON 1`): that implementation is chosen regardless of
    /// `join_algorithm`, and it consumes `max_memory_usage` as its shrink trigger, so a step-local
    /// `max_memory_usage` must reach the receiver even when `join_algorithm` excludes hash-family
    /// joins. Not a serialized setting; it only feeds getMinRequiredVersion.
    bool join_executes_as_constant_join = false;

    /// Whether `MergeJoin` supports the serializing join step's shape (its kind, strictness and
    /// single-clause ON expression), i.e. whether `join_algorithm = 'partial_merge'` /
    /// `'prefer_partial_merge'` really builds a `MergeJoin` for it and `'auto'` really builds a
    /// `JoinSwitcher`. Those implementations consume none of the version-4 settings (a `JoinSwitcher`
    /// only the compression one), so such a fragment must not be raised to version 4 just because the
    /// `join_algorithm` set also contains a hash fallback that will never be reached. Not a serialized
    /// setting; it only feeds getMinRequiredVersion. Defaults to false so a step that does not know
    /// its shape keeps the conservative version bump.
    bool join_shape_supports_merge_join = false;

    /// Generated operator[] overloads for each supported type category.
    QUERY_PLAN_SERIALIZATION_SETTINGS_SUPPORTED_TYPES(QueryPlanSerializationSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

private:
    std::unique_ptr<QueryPlanSerializationSettingsImpl> impl;
};

}
