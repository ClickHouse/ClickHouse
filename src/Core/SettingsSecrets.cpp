#include <Core/SettingsSecrets.h>
#include <Core/Field.h>
#include <Common/quoteString.h>

namespace DB::CoreSettings
{

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
