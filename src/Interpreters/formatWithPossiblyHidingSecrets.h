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

String format(const SecretHidingFormatSettings & settings);
}
