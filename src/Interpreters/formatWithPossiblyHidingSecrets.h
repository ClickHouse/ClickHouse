#pragma once
#include "Access/ContextAccess.h"
#include "Interpreters/Context.h"

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

    return settings.query.formatWithPossiblyHidingSensitiveData(settings.max_length, settings.one_line, show_secrets);
}
}
