#pragma once

#include <base/types.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

size_t computeMaxTableNameLength(const String & database_name, ContextPtr context, bool allow_experimental_drop_detached_table);
}
