#include <Common/StringSearcher.h>
#include <Common/UTF8Helpers.h>
#include <Poco/Unicode.h>

namespace DB
{

namespace
{

using UTF8SequenceBuffer = uint8_t[6];

/// Shared: resolve first character of the needle into lower/upper case bytes.
/// Returns true if the caller should set `force_fallback` and return early.
bool initFirstCharacter(
    const uint8_t * needle,
    size_t needle_size,
    bool & first_needle_symbol_is_ascii,
    uint8_t & l,
    uint8_t & u)
{
    UTF8SequenceBuffer l_seq;
    UTF8SequenceBuffer u_seq;

    if (*needle < 0x80u)
    {
        first_needle_symbol_is_ascii = true;
        l = static_cast<uint8_t>(std::tolower(*needle));
        u = static_cast<uint8_t>(std::toupper(*needle));
        return false;
    }

    auto first_u32 = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle), needle_size);

    /// Invalid UTF-8
    if (!first_u32)
    {
        /// Process it verbatim as a sequence of bytes. The clamp against `needle_size`
        /// matches the inner-loop clamp in `buildCacheBytes`: a truncated first sequence
        /// (e.g. a 1-byte needle starting with `0xE4`, where `seqLength` is 3) must not
        /// read past the needle, otherwise the `memcpy` propagates MemorySanitizer noise
        /// from uninitialized memory past the needle into `l_seq` / `u_seq`.
        size_t src_len = std::min<size_t>(needle_size, UTF8::seqLength(*needle));
        memcpy(l_seq, needle, src_len);
        memcpy(u_seq, needle, src_len);
    }
    else
    {
        uint32_t first_l_u32 = Poco::Unicode::toLower(*first_u32);
        uint32_t first_u_u32 = Poco::Unicode::toUpper(*first_u32);

        size_t length_l = UTF8::convertCodePointToUTF8(first_l_u32, reinterpret_cast<char *>(l_seq), sizeof(l_seq));
        size_t length_u = UTF8::convertCodePointToUTF8(first_u_u32, reinterpret_cast<char *>(u_seq), sizeof(u_seq));

        if (length_l != length_u)
        {
            l = l_seq[0];
            u = u_seq[0];
            return true; /// force_fallback
        }
    }

    l = l_seq[0];
    u = u_seq[0];
    return false;
}

/// Shared: build cache byte arrays from needle with case folding.
/// Returns true if force_fallback should be set (case expansion mismatch).
/// Only used by the Default cache searcher (AVX2 on x86, NEON on ARM).
#if defined(__AVX2__) || (defined(__aarch64__) && defined(__ARM_NEON))
bool buildCacheBytes(
    const uint8_t * needle,
    const uint8_t * needle_end,
    size_t cache_size,
    uint8_t * cache_l_bytes,
    uint8_t * cache_u_bytes,
    uint32_t & cachemask,
    size_t & cache_valid_len,
    size_t & cache_actual_len)
{
    UTF8SequenceBuffer l_seq;
    UTF8SequenceBuffer u_seq;

    const auto * needle_pos = needle;
    size_t cache_pos = 0;

    while (cache_pos < cache_size && needle_pos < needle_end)
    {
        size_t src_len = std::min<size_t>(needle_end - needle_pos, UTF8::seqLength(*needle_pos));
        auto c_u32 = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle_pos), src_len);

        if (c_u32)
        {
            int c_l_u32 = Poco::Unicode::toLower(*c_u32);
            int c_u_u32 = Poco::Unicode::toUpper(*c_u32);

            size_t dst_l_len = UTF8::convertCodePointToUTF8(c_l_u32, reinterpret_cast<char *>(l_seq), sizeof(l_seq));
            size_t dst_u_len = UTF8::convertCodePointToUTF8(c_u_u32, reinterpret_cast<char *>(u_seq), sizeof(u_seq));

            /// @note Unicode standard states it is a rare but possible occasion
            if (!(dst_l_len == dst_u_len && dst_u_len == src_len))
                return true; /// force_fallback
        }
        else
        {
            /// Invalid UTF-8 sequence in the middle of the needle: process bytes verbatim,
            /// matching the design of the outer if-else above for the first character.
            /// Without this, the inner loop below would read uninitialized bytes from
            /// `l_seq` / `u_seq` and insert them into `cachel` / `cacheu`, surfacing later
            /// as a MemorySanitizer use-of-uninitialized-value in `compare` / `search`.
            memcpy(l_seq, needle_pos, src_len);
            memcpy(u_seq, needle_pos, src_len);
        }

        cache_actual_len += src_len;
        if (cache_actual_len < cache_size)
            cache_valid_len += src_len;

        for (size_t j = 0; j < src_len && cache_pos < cache_size; ++j, ++cache_pos)
        {
            if (needle_pos < needle_end)
            {
                cache_l_bytes[cache_pos] = l_seq[j];
                cache_u_bytes[cache_pos] = u_seq[j];
                cachemask |= 1u << cache_pos;
                ++needle_pos;
            }
        }
    }

    return false;
}
#endif

/// Shared: trivial byte-by-byte UTF-8 case-insensitive comparison.
inline ALWAYS_INLINE bool compareTrivialUTF8(
    const UInt8 * haystack_pos,
    const UInt8 * haystack_end,
    const uint8_t * needle_pos,
    const uint8_t * needle_end)
{
    while (haystack_pos < haystack_end && needle_pos < needle_end)
    {
        auto haystack_code_point
            = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(haystack_pos), haystack_end - haystack_pos);
        auto needle_code_point
            = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(needle_pos), needle_end - needle_pos);

        /// Invalid UTF-8, should not compare equals
        if (!haystack_code_point || !needle_code_point)
            return false;

        if (Poco::Unicode::toLower(*haystack_code_point) != Poco::Unicode::toLower(*needle_code_point))
            return false;

        haystack_pos += UTF8::seqLength(*haystack_pos);
        needle_pos += UTF8::seqLength(*needle_pos);
    }

    return needle_pos == needle_end;
}

} // anonymous namespace

namespace TargetSpecific::Default
{

UTF8CaseInsensitiveSearcherImpl::UTF8CaseInsensitiveSearcherImpl(const UInt8 * needle_, size_t needle_size_)
    : needle(reinterpret_cast<const uint8_t *>(needle_))
    , needle_size(needle_size_)
{
    if (needle_size == 0)
        return;

    force_fallback = initFirstCharacter(needle, needle_size, first_needle_symbol_is_ascii, l, u);
    if (force_fallback)
        return;

#if defined(__AVX2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    patl = vecSet1(l);
    patu = vecSet1(u);

    constexpr size_t N = sizeof(Vec);
    uint8_t cache_l_bytes[N] = {};
    uint8_t cache_u_bytes[N] = {};

    force_fallback = buildCacheBytes(needle, needle_end, N, cache_l_bytes, cache_u_bytes, cachemask, cache_valid_len, cache_actual_len);
    if (force_fallback)
        return;

    cachel = vecLoad(cache_l_bytes);
    cacheu = vecLoad(cache_u_bytes);
#endif
}

bool UTF8CaseInsensitiveSearcherImpl::compareTrivial(
    const UInt8 * haystack_pos, const UInt8 * const haystack_end, const uint8_t * needle_pos) const
{
    return compareTrivialUTF8(haystack_pos, haystack_end, needle_pos, needle_end);
}

bool UTF8CaseInsensitiveSearcherImpl::compare(const UInt8 * /*haystack*/, const UInt8 * haystack_end, const UInt8 * pos) const
{
    /// An empty needle matches anywhere; `startsWith`/`endsWith` call compare directly, and without this
    /// the scalar tail would dereference `pos` (which equals `haystack_end` for an empty needle).
    if (needle == needle_end)
        return true;

#if defined(__AVX2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    constexpr size_t N = sizeof(Vec);

    if (likely(!force_fallback) && pos + N <= haystack_end)
    {
        const auto v_haystack = vecLoad(pos);
        const auto v_against_l = vecCmpeq(v_haystack, cachel);
        const auto v_against_u = vecCmpeq(v_haystack, cacheu);
        const auto v_against_l_or_u = vecOr(v_against_l, v_against_u);
        const auto mask = static_cast<uint32_t>(vecMovemask(v_against_l_or_u));

        if ((mask & cachemask) == cachemask)
        {
            if (compareTrivial(pos, haystack_end, needle))
                return true;
        }

        return false;
    }
#endif

    if (*pos == l || *pos == u)
    {
        pos += first_needle_symbol_is_ascii;
        const auto * needle_pos = needle + first_needle_symbol_is_ascii;

        if (compareTrivial(pos, haystack_end, needle_pos))
            return true;
    }

    return false;
}

const UInt8 * UTF8CaseInsensitiveSearcherImpl::search(const UInt8 * haystack, const UInt8 * const haystack_end) const
{
    if (needle_size == 0)
        return haystack;

#if defined(__AVX2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    constexpr size_t N = sizeof(Vec);

    if (unlikely(force_fallback))
        goto scalar;

    while (haystack < haystack_end)
    {
        /// The first load scans N bytes for first-char matches; the second loads N bytes
        /// from the match position (at most N-1 bytes ahead). Check 2*N upfront to cover both.
        if (haystack + 2 * N <= haystack_end)
        {
            const auto v_haystack = vecLoad(haystack);
            const auto v_against_l = vecCmpeq(v_haystack, patl);
            const auto v_against_u = vecCmpeq(v_haystack, patu);
            const auto v_against_l_or_u = vecOr(v_against_l, v_against_u);

            const auto mask = static_cast<uint32_t>(vecMovemask(v_against_l_or_u));

            if (mask == 0)
            {
                /// No first-byte candidate in this window. The needle's first byte is a UTF-8
                /// lead or ASCII byte, so it never equals a continuation byte; skip the whole
                /// window and realign to the next sequence boundary for the scalar tail.
                haystack += N;
                UTF8::syncForward(haystack, haystack_end);
                continue;
            }

            const auto offset = __builtin_ctz(mask);
            haystack += offset;

            {
                const auto v_haystack_offset = vecLoad(haystack);
                const auto v_against_l_offset = vecCmpeq(v_haystack_offset, cachel);
                const auto v_against_u_offset = vecCmpeq(v_haystack_offset, cacheu);
                const auto v_against_l_or_u_offset = vecOr(v_against_l_offset, v_against_u_offset);
                const auto mask_offset_both = static_cast<uint32_t>(vecMovemask(v_against_l_or_u_offset));

                if ((mask_offset_both & cachemask) == cachemask)
                {
                    if (compareTrivial(haystack, haystack_end, needle))
                        return haystack;
                }

                haystack += UTF8::seqLength(*haystack);
                continue;
            }
        }

        if (haystack == haystack_end)
            return haystack_end;

        if (*haystack == l || *haystack == u)
        {
            const auto * haystack_pos = haystack + first_needle_symbol_is_ascii;
            const auto * needle_pos = needle + first_needle_symbol_is_ascii;

            if (compareTrivial(haystack_pos, haystack_end, needle_pos))
                return haystack;
        }

        haystack += UTF8::seqLength(*haystack);
    }

    return haystack_end;

scalar:
#endif
    while (haystack < haystack_end)
    {
        if (*haystack == l || *haystack == u)
        {
            const auto * haystack_pos = haystack + first_needle_symbol_is_ascii;
            const auto * needle_pos = needle + first_needle_symbol_is_ascii;

            if (compareTrivial(haystack_pos, haystack_end, needle_pos))
                return haystack;
        }

        haystack += UTF8::seqLength(*haystack);
    }

    return haystack_end;
}

}

}
