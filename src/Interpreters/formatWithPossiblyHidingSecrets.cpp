#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsBool output_format_always_quote_identifiers;
    extern const SettingsIdentifierQuotingStyle output_format_identifier_quoting_style;
    extern const SettingsBool print_pretty_type_names;
}

String format(const SecretHidingFormatSettings & settings)
{
    const bool show_secrets = settings.ctx->displaySecretsInShowAndSelect()
        && settings.ctx->getSettingsRef()[Setting::format_display_secrets_in_show_and_select]
        && settings.ctx->getAccess()->isGranted(AccessType::displaySecretsInShowAndSelect);

    return settings.query.formatWithPossiblyHidingSensitiveData(
        settings.max_length,
        settings.one_line,
        show_secrets,
        settings.ctx->getSettingsRef()[Setting::print_pretty_type_names],
        settings.ctx->getSettingsRef()[Setting::output_format_always_quote_identifiers],
        settings.ctx->getSettingsRef()[Setting::output_format_identifier_quoting_style]);
}

}
