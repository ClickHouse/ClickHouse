#pragma once

#include <base/types.h>
#include <Common/maskURIPassword.h>

#include <functional>
#include <optional>
#include <unordered_map>

namespace DB
{
class Field;
}

namespace DB::CoreSettings
{

/// Rewrites the value in place and returns whether anything was masked.
using ValueMaskingFunc = std::function<bool(String &)>;

/// A URL carries its credential either as `user:password@` userinfo or, when it is presigned, in the
/// query parameters, and either form can appear in the same setting.
///
/// Scanning for a presigned parameter is only sound on a value that is one whole URL: such a value
/// ends at `&`, at `#` or at the end of the text, so on a URL embedded in a longer string it would
/// run past the end of the URL. Every setting masked with this holds one whole URL, so the
/// precondition holds by construction and needs no check.
inline bool maskURLCredentials(String & value)
{
    bool masked = maskURIPassword(&value);
    masked |= maskPresignedURLParameters(value);
    return masked;
}

/// The settings of the query-level `Settings` collection whose value can carry a credential, and how
/// each one is masked. `system.query_log.query` shows
/// `format_avro_schema_registry_url = 'http://user:[HIDDEN]@registry:8080'`, so every other place that
/// prints the same value hides the same secret through this map.
///
/// Mirrors the per-engine `SETTINGS_TO_HIDE` maps (`Kafka_fwd.h`, `NATS_fwd.h`, ...), which do this
/// for table engine settings.
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"format_avro_schema_registry_url", maskURLCredentials},
    {"url_base", maskURLCredentials},
    {"s3_base", maskURLCredentials},
};

/// Returns whether anything was masked.
inline bool maskSettingValue(const String & setting_name, String & value)
{
    auto it = SETTINGS_TO_HIDE.find(setting_name);
    return it != SETTINGS_TO_HIDE.end() && it->second(value);
}

/// Same as above, for a sink that also has the raw `Field`. A setting value can be an AST rather
/// than a literal, e.g. `custom_x = disk(type = 's3', secret_access_key = '...')`, and no plain
/// `Field` formatter hides that.
///
/// This is the form for a sink that prints the value as it is, such as a system table column or a
/// log line. A sink that prints SQL needs `renderSecretSettingValue` instead.
bool maskSettingValue(const String & setting_name, const Field & field, String & value);

/// Renders a setting value that holds a secret as the SQL text that hides it, and returns `nullopt`
/// for a value that holds none, which the caller then renders with its own visitor.
///
/// The masking runs on the raw string and the result is quoted afterwards, because the two cannot be
/// done in the other order: the value of a presigned URL parameter ends at the end of the text, so
/// masking an already-quoted literal takes the closing quote with it and leaves
/// `s3_base = 'https://bucket/f.csv?X-Amz-Signature=[HIDDEN]`, which no longer parses.
///
/// Whether a value holds a secret and how that secret is hidden are the same question, so a
/// `formatImpl` and the matching `hasSecretParts` both ask it here and cannot disagree.
std::optional<String> renderSecretSettingValue(const String & setting_name, const Field & value);

}
