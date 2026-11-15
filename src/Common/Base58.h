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
///
/// `dst` also holds the conversion's intermediate state: it must have room for
/// `2 * src_length + 1` bytes to encode and `src_length` bytes to decode.
size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});
std::optional<size_t> decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst, const std::function<void()> & check_cancellation = {});

/// Maximum base58-encoded lengths for fixed-size inputs.
/// A 32-byte value uses 9 intermediate digits of radix 58^5, producing at most
/// 9*5 = 45 raw base58 digits; the leading digit is always zero, so max output is 44.
/// Similarly, 64 bytes use 18 intermediate digits: 18*5 = 90, minus 2 guaranteed
/// leading zeros, giving max output 88.
constexpr auto BASE58_ENCODED_32_LEN = 44UL;
constexpr auto BASE58_ENCODED_64_LEN = 88UL;

/// The same bound for an arbitrary body length: 1366/1000 exceeds 8/log2(58), so the largest
/// `body_length`-byte value needs at most this many digits. `body_length` is the matching lower
/// bound, because a leading zero byte takes one '1' and any other byte takes at least one digit.
constexpr size_t maxBase58EncodedLength(size_t body_length)
{
    return body_length * 1366 / 1000 + 1;
}

static_assert(maxBase58EncodedLength(32) == BASE58_ENCODED_32_LEN);
static_assert(maxBase58EncodedLength(64) == BASE58_ENCODED_64_LEN);

size_t encodeBase58_32(const UInt8 * src, UInt8 * dst);
size_t encodeBase58_64(const UInt8 * src, UInt8 * dst);

}
