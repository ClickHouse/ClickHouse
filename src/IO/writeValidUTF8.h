#pragma once

#include <base/types.h>

#include <cstring>

namespace DB
{

class WriteBuffer;

/// Skips whole 16-byte blocks of ASCII: returns the first such block with a byte of `0x80` or more, or the
/// position where fewer than 16 bytes are left. Checks 64-byte blocks first, so long ASCII runs are skipped
/// with the widest vectors of the target.
inline const char * skipASCIIBlocks(const char * p, const char * end)
{
    using Bytes = Int8 __attribute__((ext_vector_type(16)));
    using Mask = bool __attribute__((ext_vector_type(16)));

    /// `bytes >> 7` is nonzero exactly where the sign bit (`bytes < 0`) is set: a comparison would depend
    /// on `-faltivec-src-compat` on PowerPC. Only the sign bits are tested: `vpmovmskb` on x86, `cmlt`
    /// plus `umaxv` on NEON.
    auto has_non_ascii = [](Bytes bytes) { return __builtin_reduce_or(__builtin_convertvector(bytes >> 7, Mask)); };

    for (Bytes bytes[4]; end - p >= 64; p += 64)
    {
        memcpy(bytes, p, sizeof(bytes));
        if (has_non_ascii(bytes[0] | bytes[1] | bytes[2] | bytes[3]))
            break;
    }

    for (Bytes bytes; end - p >= 16; p += 16)
    {
        memcpy(&bytes, p, sizeof(bytes));
        if (has_non_ascii(bytes))
            break;
    }

    return p;
}

/// Writes `[begin, end)` to `out` with every invalid UTF-8 sequence replaced by U+FFFD, collapsing a run of
/// them into one. Allocates nothing of its own, unlike `WriteBufferValidUTF8`, which buffers into 4 KiB of
/// its own memory so that it can carry a sequence split across two writes. Use this where the whole value is
/// already in hand and that state is not needed.
void writeValidUTF8(const char * begin, const char * end, WriteBuffer & out);

}
