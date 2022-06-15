#pragma once

#include "config_core.h"

#include <base/types.h>

int kerberosInit(const String & keytab_file, const String & principal, const String & cache_name = "");
