#pragma once

#include <base/types.h>
#include <Common/maskURIPassword.h>

#include <functional>
#include <unordered_map>

namespace DB::CoreSettings
{

/// Rewrites the value in place and returns whether anything was masked.
using ValueMaskingFunc = std::function<bool(String &)>;

/// The settings of the query-level `Settings` collection whose value can carry a credential, and how
/// each one is masked. `system.query_log.query` shows
/// `format_avro_schema_registry_url = 'http://user:[HIDDEN]@registry:8080'`, so every other place that
/// prints the same value hides the same secret through this map.
///
/// Mirrors the per-engine `SETTINGS_TO_HIDE` maps (`Kafka_fwd.h`, `NATS_fwd.h`, ...), which do this
/// for table engine settings.
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"format_avro_schema_registry_url", [](String & value) { return maskURIPassword(&value); }},
    {"url_base", [](String & value) { return maskURIPassword(&value); }},
    {"s3_base", [](String & value) { return maskURIPassword(&value); }},
};

/// Returns whether anything was masked.
inline bool maskSettingValue(const String & setting_name, String & value)
{
    auto it = SETTINGS_TO_HIDE.find(setting_name);
    return it != SETTINGS_TO_HIDE.end() && it->second(value);
}

}
