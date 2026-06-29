#pragma once

/// Portable, clean-room PForDelta integer codec, generic over T (UInt32/UInt64).

#include <Compression/PFor/bulk.h>

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

/// Inverse of encodeBlocks; decodes `count` values and returns bytes consumed from `in`.
template <typename T>
inline size_t decodeBlocks(const uint8_t * in, size_t count, Delta mode, T * out) noexcept
{
    return detail::bulkDecode<T>(in, count, mode, out);
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

/// Decompress a self-describing buffer into `out` (>= original count); returns the count.
template <typename T>
inline size_t decompressInto(std::span<const uint8_t> in, T * out) noexcept
{
    const uint8_t * p = in.data();
    uint64_t count = 0;
    p += detail::getVarint(p, count);
    const uint8_t flags = *p++;
    decodeBlocks<T>(p, static_cast<size_t>(count), static_cast<Delta>(flags & 3u), out);
    return static_cast<size_t>(count);
}

/// Number of values stored in a self-describing buffer (reads only the header).
inline size_t decompressedCount(std::span<const uint8_t> in) noexcept
{
    uint64_t count = 0;
    detail::getVarint(in.data(), count);
    return static_cast<size_t>(count);
}

/// Allocating convenience wrappers around compressInto/decompressInto.
template <typename T>
inline std::vector<uint8_t> compress(std::span<const T> in, Delta mode)
{
    std::vector<uint8_t> out(maxCompressedBytes<T>(in.size()));
    out.resize(compressInto<T>(in, mode, out.data()));
    return out;
}

template <typename T>
inline std::vector<T> decompress(std::span<const uint8_t> in)
{
    std::vector<T> out(decompressedCount(in));
    decompressInto<T>(in, out.data());
    return out;
}

}
