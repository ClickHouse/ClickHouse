#pragma once

#include <cstdint>

namespace DB
{

enum class QueryCacheUsage : uint8_t
{
    Unknown,  /// we don't know what what happened
    None,     /// query result neither written nor read into/from query cache
    Write,    /// query result written into query cache
    Read,     /// query result read from query cache
};

}
