#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/IAST.h>

namespace DB
{
namespace Setting
{
    extern const SettingsIdentifierQuotingRule show_create_query_identifier_quoting_rule;
    extern const SettingsIdentifierQuotingStyle show_create_query_identifier_quoting_style;
    extern const SettingsBool print_pretty_type_names;
}

String format(const SecretHidingFormatSettings & settings)
{
    const bool show_secrets = settings.ctx->canDisplaySecretsInShowAndSelect();

    return settings.query.formatWithPossiblyHidingSensitiveData(
        settings.max_length,
        settings.one_line,
        show_secrets,
        settings.ctx->getSettingsRef()[Setting::print_pretty_type_names],
        settings.ctx->getSettingsRef()[Setting::show_create_query_identifier_quoting_rule],
        settings.ctx->getSettingsRef()[Setting::show_create_query_identifier_quoting_style]);
}

}
