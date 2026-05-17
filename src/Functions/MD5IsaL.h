#pragma once

#include <Columns/ColumnFixedString.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <cstddef>

#include "config.h"

#if USE_ISAL_CRYPTO
#    include <isa-l_crypto/md5_mb.h>

#    include <algorithm>
#    include <limits>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace MD5IsaL
{

inline constexpr size_t digest_size = 16;

#if USE_ISAL_CRYPTO
inline constexpr bool enabled = true;

/// Below this batch size the multi-buffer manager cannot fill even one SIMD lane,
/// so scalar OpenSSL beats it after manager/CTX bookkeeping is accounted for.
inline constexpr size_t min_batch_size = ISAL_MD5_MIN_LANES;

/// Widest supported lane width. The pool is sized to this so a single wave can
/// saturate the AVX-512 dispatch (32 lanes) without dynamic allocation.
inline constexpr size_t lane_count = ISAL_MD5_MAX_LANES;

[[noreturn]] inline void throwError(const char * operation, int status)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "ISA-L Crypto MD5 {} failed with status {}", operation, status);
}

inline void writeLE32(UInt8 * out, UInt32 value)
{
    out[0] = static_cast<UInt8>(value);
    out[1] = static_cast<UInt8>(value >> 8);
    out[2] = static_cast<UInt8>(value >> 16);
    out[3] = static_cast<UInt8>(value >> 24);
}

inline void writeCompletedDigest(const ISAL_MD5_HASH_CTX * completed, ColumnFixedString::Chars & chars_to)
{
    /// The row index is stashed directly in user_data via integer-to-pointer cast,
    /// so completion order can differ from submission order without a side table.
    const auto row = reinterpret_cast<size_t>(completed->user_data);
    UInt8 * out = &chars_to[row * digest_size];

    for (size_t word = 0; word < ISAL_MD5_DIGEST_NWORDS; ++word)
        writeLE32(out + word * sizeof(UInt32), completed->job.result_digest[word]);
}

template <typename GetRow>
bool tryApply(size_t input_rows_count, ColumnFixedString::Chars & chars_to, const GetRow & get_row)
{
    if (input_rows_count == 0)
        return true;

    if (input_rows_count < min_batch_size)
        return false;

    /// ISA-L's per-job length is `uint32_t`. A single oversized row forces the
    /// whole batch back to the scalar fallback rather than splitting the work.
    for (size_t row = 0; row < input_rows_count; ++row)
    {
        const UInt8 * begin = nullptr;
        size_t size = 0;
        get_row(row, begin, size);
        if (size > std::numeric_limits<UInt32>::max())
            return false;
    }

    ISAL_MD5_HASH_CTX_MGR manager;
    if (const int status = isal_md5_ctx_mgr_init(&manager); status != 0)
        throwError("manager init", status);

    /// Fixed-size pool keeps memory bounded regardless of `input_rows_count`.
    ISAL_MD5_HASH_CTX pool[lane_count];
    for (auto & ctx : pool)
        isal_hash_ctx_init(&ctx);

    /// Empty rows still need a non-null pointer; ISA-L treats a zero `len`
    /// as "hash nothing" but defensive callers may dereference the buffer.
    const UInt8 empty_input = 0;

    for (size_t base = 0; base < input_rows_count; base += lane_count)
    {
        const size_t wave = std::min(lane_count, input_rows_count - base);

        for (size_t slot = 0; slot < wave; ++slot)
        {
            const UInt8 * begin = nullptr;
            size_t size = 0;
            get_row(base + slot, begin, size);
            if (size == 0)
                begin = &empty_input;

            pool[slot].user_data = reinterpret_cast<void *>(base + slot);

            ISAL_MD5_HASH_CTX * completed = nullptr;
            if (const int status = isal_md5_ctx_mgr_submit(
                    &manager, &pool[slot], &completed, begin, static_cast<UInt32>(size), ISAL_HASH_ENTIRE);
                status != 0)
                throwError("submit", status);

            if (completed)
                writeCompletedDigest(completed, chars_to);
        }

        while (true)
        {
            ISAL_MD5_HASH_CTX * completed = nullptr;
            if (const int status = isal_md5_ctx_mgr_flush(&manager, &completed); status != 0)
                throwError("flush", status);

            if (!completed)
                break;

            writeCompletedDigest(completed, chars_to);
        }
    }

    return true;
}
#else
inline constexpr bool enabled = false;
inline constexpr size_t min_batch_size = 0;

template <typename GetRow>
bool tryApply(size_t, ColumnFixedString::Chars &, const GetRow &)
{
    return false;
}
#endif

}
}
