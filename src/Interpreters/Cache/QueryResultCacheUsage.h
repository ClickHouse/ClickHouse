#pragma once

#include <cstdint>

namespace DB
{

enum class QueryResultCacheUsage : uint8_t
{
    Unknown,  /// we don't know what what happened
    None,     /// query result neither written nor read into/from query result cache
    Write,    /// query result written into query result cache
    Read,     /// query result read from query result cache
};

}
