#pragma once

/// Portable, clean-room PForDelta integer codec, generic over T (UInt32/UInt64).

#include <Compression/PFor/bulk.h>

#include <optional>
#include <span>
#include <vector>

namespace DB::PFor
{

using detail::BLOCK;

/// Worst-case compressed byte count for `count` values of type T.
template <typename T>
inline size_t maxCompressedBytes(size_t count) noexcept
{
    return sizeof(T) * count + 2 * (count / BLOCK + 1) + 16;
}

/// Headerless block stream (the caller supplies count and mode); returns bytes written.
template <typename T>
inline size_t encodeBlocks(std::span<const T> in, Delta mode, uint8_t * out) noexcept
{
    return detail::bulkEncode<T>(in.data(), in.size(), mode, out);
}

/// Inverse of encodeBlocks; returns bytes consumed. Pass `end` to decode fail-closed (reads bounded, returns 0 on corrupt input); nullptr keeps the unchecked fast path.
template <typename T>
inline size_t decodeBlocks(const uint8_t * in, size_t count, Delta mode, T * out, const uint8_t * end = nullptr) noexcept
{
    return detail::bulkDecode<T>(in, count, mode, out, end);
}

/// Self-describing compress into a caller buffer (>= maxCompressedBytes<T>): [varint count][u8 flags][block stream].
template <typename T>
inline size_t compressInto(std::span<const T> in, Delta mode, uint8_t * out) noexcept
{
    uint8_t * p = out;
    p += detail::putVarint(p, in.size());
    *p++ = static_cast<uint8_t>(static_cast<unsigned>(mode) | (sizeof(T) == 8 ? 4u : 0u));
    p += encodeBlocks<T>(in, mode, p);
    return static_cast<size_t>(p - out);
}

/// A decoded self-describing header: value count, delta mode, and the [body, end) block stream.
struct Header
{
    uint64_t count = 0;
    Delta mode = Delta::none;
    const uint8_t * body = nullptr;
    const uint8_t * end = nullptr;
};

/// Parse the self-describing header fail-closed (bounded varint + validated flag byte); nullopt on
/// truncation, an undefined flag bit, an invalid mode, or an element-width (UInt32/UInt64) mismatch.
template <typename T>
inline std::optional<Header> parseHeader(std::span<const uint8_t> in) noexcept
{
    Header h;
    h.end = in.data() + in.size();
    const uint8_t * p = detail::getVarintChecked(in.data(), h.end, h.count);
    if (!p || p >= h.end)
        return std::nullopt;
    const uint8_t flags = *p++;
    if (flags & ~0x7u)                            // only mode (bits 0-1) and width (bit 2) are defined
        return std::nullopt;
    if ((flags & 3u) == 3u)                       // mode must be none/d0/d1
        return std::nullopt;
    if (((flags & 4u) != 0) != (sizeof(T) == 8))  // stored element width must match T
        return std::nullopt;
    h.mode = static_cast<Delta>(flags & 3u);
    h.body = p;
    return h;
}

/// Decompress a self-describing buffer into `out`; returns the value count, or 0 on malformed/truncated input.
template <typename T>
inline size_t decompressInto(std::span<const uint8_t> in, T * out) noexcept
{
    const std::optional<Header> h = parseHeader<T>(in);
    if (!h || h->count == 0)
        return 0;
    if (decodeBlocks<T>(h->body, static_cast<size_t>(h->count), h->mode, out, h->end) == 0)
        return 0;
    return static_cast<size_t>(h->count);
}

/// Number of values stored in a self-describing buffer (reads only the header); 0 if truncated.
inline size_t decompressedCount(std::span<const uint8_t> in) noexcept
{
    uint64_t count = 0;
    if (!detail::getVarintChecked(in.data(), in.data() + in.size(), count))
        return 0;
    return static_cast<size_t>(count);
}

/// Allocating convenience wrappers around compressInto/decompressInto.
template <typename T>
inline std::vector<uint8_t> compress(std::span<const T> in, Delta mode) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<uint8_t> out(maxCompressedBytes<T>(in.size())); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    out.resize(compressInto<T>(in, mode, out.data()));
    return out;
}

/// Fail-closed: returns {} on any malformed input (bad header or truncated body).
template <typename T>
inline std::vector<T> decompress(std::span<const uint8_t> in) // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    const std::optional<Header> h = parseHeader<T>(in);
    if (!h)
        return {};
    /// A block holds at most BLOCK values per byte, so a larger count cannot fit the body — reject before allocating.
    if (h->count > static_cast<uint64_t>(h->end - h->body) * BLOCK)
        return {};
    std::vector<T> out(static_cast<size_t>(h->count)); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (h->count && decodeBlocks<T>(h->body, static_cast<size_t>(h->count), h->mode, out.data(), h->end) == 0)
        return {};
    return out;
}

}
