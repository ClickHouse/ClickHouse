#pragma once

#include "config_core.h"

#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int KERBEROS_ERROR;
}
}

int kerberosInit(const String & keytab_file, const String & principal, const String & cache_name = "");
