#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Common/SettingsChanges.h>

namespace DB
{
class ASTStorage;
struct KafkaSettingsImpl;

const auto KAFKA_RESCHEDULE_MS = 500;
const auto KAFKA_CLEANUP_TIMEOUT_MS = 3000;
// once per minute leave do reschedule (we can't lock threads in pool forever)
const auto KAFKA_MAX_THREAD_WORK_DURATION_MS = 60000;
// 10min
const auto KAFKA_CONSUMERS_POOL_TTL_MS_MAX = 600'000;

/// List of available types supported in RabbitMQSettings object
#define KAFKA_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
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
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

KAFKA_SETTINGS_SUPPORTED_TYPES(KafkaSettings, DECLARE_SETTING_TRAIT)

/** Settings for the Kafka engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct KafkaSettings
{
    KafkaSettings();
    KafkaSettings(const KafkaSettings & settings);
    KafkaSettings(KafkaSettings && settings) noexcept;
    ~KafkaSettings();

    KAFKA_SETTINGS_SUPPORTED_TYPES(KafkaSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    void loadFromNamedCollection(const MutableNamedCollectionPtr & named_collection);

    SettingsChanges getFormatSettings() const;

    void sanityCheck() const;

private:
    std::unique_ptr<KafkaSettingsImpl> impl;
};

}
