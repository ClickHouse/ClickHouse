#include <Storages/maskEngineSettingValue.h>

#include <Core/Field.h>
#include <Core/SettingsSecrets.h>
#include <Parsers/engineSettingsToHide.h>

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
            /// A registry names a setting that *may* carry a secret and decides per value whether
            /// this one does - a URL without credentials in it does not - so `nullopt` means there
            /// is nothing to hide. The first registry that knows the name answers, as
            /// `renderSecretChangeValue` in `ASTSetQuery.cpp` does, so the two cannot disagree on what is
            /// secret.
            auto rendered = it->second(field);
            if (!rendered)
                return {};

            /// Some rules answer with whatever they are given - a URI with no password in it comes back as it
            /// went in. Nothing was hidden then, and a row saying its value is a placeholder would be lying.
            return *rendered == value ? String{} : std::move(*rendered);
        }
    }

    return {};
}

}
