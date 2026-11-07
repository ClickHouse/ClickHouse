#pragma once

#include <base/types.h>

#include <functional>
#include <optional>


namespace DB
{

/// The generic (variable-length) Base58 encoder and decoder use a big-integer base conversion
/// whose cost is quadratic in the input length. For large inputs this can run for a very long
/// time, so they accept an optional `check_cancellation` callback that is invoked periodically;
/// it is expected to throw if the query has been cancelled or exceeded its time limit.
size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});
std::optional<size_t> decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});

/// Maximum base58-encoded lengths for fixed-size inputs.
/// A 32-byte value uses 9 intermediate digits of radix 58^5, producing at most
/// 9*5 = 45 raw base58 digits; the leading digit is always zero, so max output is 44.
/// Similarly, 64 bytes use 18 intermediate digits: 18*5 = 90, minus 2 guaranteed
/// leading zeros, giving max output 88.
constexpr auto BASE58_ENCODED_32_LEN = 44UL;
constexpr auto BASE58_ENCODED_64_LEN = 88UL;

size_t encodeBase58_32(const UInt8 * src, UInt8 * dst);
size_t encodeBase58_64(const UInt8 * src, UInt8 * dst);
std::optional<size_t> decodeBase58_32(const UInt8 * src, size_t src_length, UInt8 * dst);
std::optional<size_t> decodeBase58_64(const UInt8 * src, size_t src_length, UInt8 * dst);

}
