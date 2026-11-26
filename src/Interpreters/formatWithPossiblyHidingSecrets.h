#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{
class IAST;

struct SecretHidingFormatSettings
{
    // We can't store const Context& as there's a dangerous usage {.ctx = *getContext()}
    // which is UB in case getContext()'s return ptr is the only one holding the object
    const ContextPtr & ctx;
    const IAST & query;
    size_t max_length = 0;
    bool one_line = true;
};

/// Whether this user may see plaintext secrets: the server setting, the format setting and the
/// `displaySecretsInShowAndSelect` grant all have to allow it.
bool canDisplaySecrets(const ContextPtr & context);

String format(const SecretHidingFormatSettings & settings);
}
