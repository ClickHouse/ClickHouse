#pragma once

#include "config_core.h"

#include <base/types.h>

#if USE_KRB5

void kerberosInit(const String & keytab_file, const String & principal, const String & cache_name = "");

#endif // USE_KRB5
