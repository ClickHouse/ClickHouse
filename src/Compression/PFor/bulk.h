#pragma once

// Block-stream framing + LEB128 varints + the delta transform (fused into encode/decode).
// Independently authored. A stream is a sequence of fixed BLOCK-value blocks; the last
// block may be partial. Counts and delta mode are supplied by the caller (headerless).

#include <Compression/PFor/block.h>
#include <Compression/PFor/common.h>

namespace DB::PFor::detail
{

/// LEB128 unsigned varint.
inline ALWAYS_INLINE size_t putVarint(uint8_t * p, uint64_t v) noexcept
{
    size_t n = 0;
    while (v >= 0x80)
    {
        p[n++] = static_cast<uint8_t>(v | 0x80u);
        v >>= 7;
    }
    p[n++] = static_cast<uint8_t>(v);
    return n;
}

inline ALWAYS_INLINE size_t getVarint(const uint8_t * p, uint64_t & v) noexcept
{
    v = 0;
    unsigned shift = 0;
    size_t n = 0;
    uint8_t b = 0;
    do
    {
        b = p[n++];
        v |= static_cast<uint64_t>(b & 0x7Fu) << shift;
        shift += 7;
    } while (b & 0x80u);
    return n;
}

template <typename T>
inline size_t bulkEncode(const T * in, size_t n, Delta mode, uint8_t * out) noexcept
{
    if (n == 0)
        return 0;
    uint8_t * p = out;
    T prev = 0;
    T r[BLOCK];
    for (size_t s = 0; s < n; s += BLOCK)
    {
        const unsigned cnt = static_cast<unsigned>((n - s < BLOCK) ? (n - s) : BLOCK);
        switch (mode)
        {
            case Delta::none:
                for (unsigned i = 0; i < cnt; ++i)
                    r[i] = in[s + i];
                break;
            case Delta::d0:
                for (unsigned i = 0; i < cnt; ++i)
                {
                    r[i] = static_cast<T>(in[s + i] - prev);
                    prev = in[s + i];
                }
                break;
            case Delta::d1:
                for (unsigned i = 0; i < cnt; ++i)
                {
                    r[i] = static_cast<T>(in[s + i] - prev - 1);
                    prev = in[s + i];
                }
                break;
        }
        p += blockEncode<T>(r, cnt, p);
    }
    return static_cast<size_t>(p - out);
}

template <typename T>
inline size_t bulkDecode(const uint8_t * in, size_t count, Delta mode, T * out) noexcept
{
    if (count == 0)
        return 0;
    const uint8_t * p = in;
    T prev = 0;
    for (size_t s = 0; s < count; s += BLOCK)
    {
        const unsigned cnt = static_cast<unsigned>((count - s < BLOCK) ? (count - s) : BLOCK);
        // blockDecode reconstructs delta in-place (fused single pass when possible),
        // threading the running carry through `prev`.
        p += blockDecode<T>(p, cnt, out + s, mode, prev);
    }
    return static_cast<size_t>(p - in);
}

}
