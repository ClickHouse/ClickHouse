#pragma once

#include <Columns/ColumnFixedString.h>
#include <base/types.h>
#include <Common/Exception.h>

#include <cstddef>

#include "config.h"

#if USE_ISAL_CRYPTO
#    include <isa-l_crypto/md5_mb.h>

#    include <limits>
#    include <vector>
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

inline void throwError(const char * operation, int status)
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
    const auto row = *static_cast<const size_t *>(completed->user_data);
    UInt8 * out = &chars_to[row * digest_size];

    for (size_t word = 0; word < ISAL_MD5_DIGEST_NWORDS; ++word)
        writeLE32(out + word * sizeof(UInt32), completed->job.result_digest[word]);
}

template <typename GetRow>
bool tryApply(size_t input_rows_count, ColumnFixedString::Chars & chars_to, const GetRow & get_row)
{
    if (input_rows_count == 0)
        return true;

    const UInt8 empty_input = 0;
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

    std::vector<ISAL_MD5_HASH_CTX> contexts(input_rows_count);
    std::vector<size_t> row_indices(input_rows_count);

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        const UInt8 * begin = nullptr;
        size_t size = 0;
        get_row(row, begin, size);
        begin = size == 0 ? &empty_input : begin;

        isal_hash_ctx_init(&contexts[row]);
        row_indices[row] = row;
        contexts[row].user_data = &row_indices[row];

        ISAL_MD5_HASH_CTX * completed = nullptr;
        if (const int status
            = isal_md5_ctx_mgr_submit(&manager, &contexts[row], &completed, begin, static_cast<UInt32>(size), ISAL_HASH_ENTIRE);
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

    return true;
}
#else
inline constexpr bool enabled = false;

template <typename GetRow>
bool tryApply(size_t, ColumnFixedString::Chars &, const GetRow &)
{
    return false;
}
#endif

}
}
