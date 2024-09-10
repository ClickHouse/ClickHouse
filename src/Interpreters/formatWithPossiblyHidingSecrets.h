#pragma once

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>


#include <Core/Settings.h>
#include <Parsers/IdentifierQuotingRule.h>

namespace DB
{

struct SecretHidingFormatSettings
{
    // We can't store const Context& as there's a dangerous usage {.ctx = *getContext()}
    // which is UB in case getContext()'s return ptr is the only one holding the object
    const ContextPtr & ctx;
    const IAST & query;
    size_t max_length = 0;
    bool one_line = true;
};

inline String format(const SecretHidingFormatSettings & settings)
{
    const bool show_secrets = settings.ctx->displaySecretsInShowAndSelect()
        && settings.ctx->getSettingsRef().format_display_secrets_in_show_and_select
        && settings.ctx->getAccess()->isGranted(AccessType::displaySecretsInShowAndSelect);

    IdentifierQuotingRule identifier_quoting_rule = settings.ctx->getSettingsRef().output_format_always_quote_identifiers
        ? IdentifierQuotingRule::AlwaysQuote
        : IdentifierQuotingRule::WhenNecessaryAndAvoidAmbiguity;
    return settings.query.formatWithPossiblyHidingSensitiveData(
        settings.max_length,
        settings.one_line,
        show_secrets,
        settings.ctx->getSettingsRef().print_pretty_type_names,
        identifier_quoting_rule,
        settings.ctx->getSettingsRef().output_format_identifier_quoting_style);
}

}
