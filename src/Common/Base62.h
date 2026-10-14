#pragma once

#include <base/types.h>

#include <functional>
#include <optional>


namespace DB
{

/// The Base62 encoder and decoder use a big-integer base conversion whose cost is quadratic
/// in the input length. For large inputs this can run for a very long time, so they accept
/// an optional `check_cancellation` callback that is invoked periodically; it is expected to
/// throw if the query has been cancelled or exceeded its time limit.
size_t encodeBase62(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});
std::optional<size_t> decodeBase62(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});

}
