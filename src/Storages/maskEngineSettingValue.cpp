#include <Storages/maskEngineSettingValue.h>

#include <Core/Field.h>
#include <Core/SettingsSecrets.h>
#include <Parsers/engineSettingsToHide.h>
#include <Storages/SettingDescription.h>

namespace DB
{

String maskEngineSettingValue(const String & setting_name, const Field & field, const String & value)
{
    /// A core setting can appear in a table's settings too - `format_avro_schema_registry_url` is a
    /// format setting any engine reading Avro may carry - and this also covers a value that is an
    /// AST rather than a literal.
    String masked = value;
    if (CoreSettings::maskSettingValue(setting_name, field, masked))
        return masked == value ? String{} : masked;

    for (const auto * registry : engineSettingsToHide())
    {
        if (auto it = registry->find(setting_name); it != registry->end())
        {
            /// A registry names a setting that *may* carry a secret and decides per value, `nullopt` meaning there is
            /// nothing to hide. The first registry that knows the name answers, as in `renderSecretChangeValue`.
            auto rendered = it->second(field);
            if (!rendered)
                return {};

            /// A rule may return its input unchanged - a URI with no password - and then nothing was hidden.
            return *rendered == value ? String{} : std::move(*rendered);
        }
    }

    return {};
}

String maskEngineSettingValue(const SettingDescription & setting, const Field & field)
{
    String masked = maskEngineSettingValue(setting.name, field, setting.value);
    for (const auto alias : setting.aliases)
    {
        if (!masked.empty())
            break;
        masked = maskEngineSettingValue(String{alias}, field, setting.value);
    }
    return masked;
}

}
