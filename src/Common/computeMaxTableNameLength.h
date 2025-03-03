#pragma once

#include <base/types.h>
#include <Interpreters/Context.h>

namespace DB
{

size_t computeMaxTableNameLength(const String & database_name, ContextPtr context);
}
