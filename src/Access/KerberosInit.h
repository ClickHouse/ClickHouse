#pragma once

#include "config.h"

#include <base/types.h>

#if USE_KRB5

/// Obtains a ticket-granting ticket for `principal` from `keytab_file` into the credentials cache, or renews
/// the one there. Does nothing if the cache already has one with more than a fifth of its lifetime left,
/// unless `force` is set.
void kerberosInit(const String & keytab_file, const String & principal, const String & cache_name = "", bool force = false);

#endif // USE_KRB5
