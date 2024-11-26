#pragma once

#include <base/types.h>

/// Return most strict (by hard limit) system (non query-level, i.e. server/user/merges/...) memory limit
UInt64 getMostStrictAvailableSystemMemory();

/// Return current query tracked memory usage
Int64 getCurrentQueryMemoryUsage();
