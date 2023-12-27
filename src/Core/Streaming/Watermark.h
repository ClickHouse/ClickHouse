#pragma once

#include <base/types.h>
#include <limits>

namespace DB
{
namespace Streaming
{
/// For DateTime64 (i.e. Int64), supported range of values:
/// [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
/// The 1970-01-01 00:00:00 is Int64 value 0, earlier times are negative, so we need to use signed Int64 as watermark
constexpr Int64 INVALID_WATERMARK = std::numeric_limits<Int64>::min();
constexpr Int64 TIMEOUT_WATERMARK = std::numeric_limits<Int64>::max();
}
}
