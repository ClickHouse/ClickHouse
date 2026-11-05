#include <Core/SettingsSecrets.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <Common/maskURIPassword.h>
#include <Common/quoteString.h>

#include <functional>
#include <unordered_map>

namespace DB::CoreSettings
{

namespace
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
bool maskURLCredentials(String & value)
{
    /// A statement is masked for logging before its settings are validated, so a value that no URI
    /// parser can read still reaches a log.
    if (findURIAuthority(value) == String::npos && value.contains('@'))
    {
        value = HIDDEN_SECRET;
        return true;
    }

    /// `maskURIUserinfo` ends the authority where the RFC does, but the consumer of the value may read a
    /// wider userinfo, as the MongoDB driver does in `mongodb://u:1?pw@1@host`.
    bool masked = maskURIUserinfo(value) || maskURIPassword(&value);
    masked |= maskPresignedURLParameters(value);
    return masked;
}

/// The settings of the query-level `Settings` collection whose value can carry a credential, and how
/// each one is masked.
///
/// Mirrors the per-engine `SETTINGS_TO_HIDE` maps (`Kafka_fwd.h`, `NATS_fwd.h`, ...), which do this
/// for table engine settings.
const std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"format_avro_schema_registry_url", maskURLCredentials},
    {"url_base", maskURLCredentials},
    {"s3_base", maskURLCredentials},
};

}

bool maskSettingValue(const String & setting_name, String & value)
{
    auto it = SETTINGS_TO_HIDE.find(setting_name);
    return it != SETTINGS_TO_HIDE.end() && it->second(value);
}

bool maskSettingValue(const String & setting_name, const Field & field, String & value)
{
    CustomType custom;
    if (field.tryGet<CustomType>(custom) && custom.isSecret())
    {
        value = custom.toString(/* show_secrets */ false);
        return true;
    }
    return maskSettingValue(setting_name, value);
}

std::optional<String> renderSecretSettingValue(const String & setting_name, const Field & value)
{
    CustomType custom;
    if (value.tryGet<CustomType>(custom) && custom.isSecret())
        return custom.toString(/* show_secrets */ false);

    /// The value need not be a String: a valueless `SETTINGS format_avro_schema_registry_url` carries
    /// Bool `true`, and the AST JSON path can carry any `Field` type. This runs before any settings
    /// validation - `executeQueryImpl` masks the query for logging first - so demanding a String here
    /// would report `BAD_GET` instead of the setting's own `TYPE_MISMATCH`.
    String str;
    if (value.tryGet<String>(str) && maskSettingValue(setting_name, str))
        return quoteString(str);

    return {};
}

}
