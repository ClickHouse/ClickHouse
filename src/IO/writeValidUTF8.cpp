#include <IO/writeValidUTF8.h>

#include "config.h"

#include <IO/WriteBuffer.h>
#include <Poco/UTF8Encoding.h>

#include <base/types.h>

#if USE_SIMDUTF
#    include <simdutf.h>
#endif

#include <bit>
#include <string_view>

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <base/simd.h>
#    include <arm_neon.h>
#endif

namespace DB
{

extern const UInt8 length_of_utf8_sequence[256];

void writeValidUTF8(const char * begin, const char * end, WriteBuffer & out)
{
#if USE_SIMDUTF
    /// Avoid runtime dispatch overhead on short strings.
    static constexpr size_t SIMDUTF_MIN_SIZE = 128;
    const size_t size = static_cast<size_t>(end - begin);
    if (size >= SIMDUTF_MIN_SIZE)
    {
        const auto validation = simdutf::validate_utf8_with_errors(begin, size);
        if (validation.error == simdutf::SUCCESS)
        {
            out.write(begin, size);
            return;
        }

        if (validation.count != 0)
            out.write(begin, validation.count);
        begin += validation.count;
    }
#endif

    static constexpr std::string_view replacement = "\xEF\xBF\xBD";

    const char * p = begin;
    const char * valid_start = begin;

    /// The last recorded character was `replacement`.
    bool just_put_replacement = false;

    auto put_valid = [&out, &just_put_replacement](const char * data, size_t len)
    {
        if (len == 0)
            return;
        just_put_replacement = false;
        out.write(data, len);
    };

    auto put_replacement = [&out, &just_put_replacement]()
    {
        if (just_put_replacement)
            return;
        just_put_replacement = true;
        out.write(replacement.data(), replacement.size());
    };

    while (p < end)
    {
#if defined(__aarch64__) && defined(__ARM_NEON)
        /// Dense Unicode needs no ASCII scan.
        if (static_cast<unsigned char>(*p) < 0x80)
        {
            p = skipASCIIBlocks(p, end);
            if (end - p >= 16)
            {
                /// The first remaining block contains a non-ASCII byte.
                const auto mask = getNibbleMask(vcgeq_u8(vld1q_u8(reinterpret_cast<const uint8_t *>(p)), vdupq_n_u8(0x80)));
                p += std::countr_zero(mask) / 4;
            }

            /// Skip ASCII in a partial vector tail without a Poco legality check.
            while (p < end && static_cast<unsigned char>(*p) < 0x80)
                ++p;
            if (p == end)
                break;
        }
#else
        /// Fast skip of ASCII
        p = skipASCIIBlocks(p, end);
        if (!(p < end))
            break;
#endif

        size_t len = length_of_utf8_sequence[static_cast<unsigned char>(*p)];

        if (len > 4)
        {
            /// Invalid start of sequence. Skip one byte.
            put_valid(valid_start, p - valid_start);
            put_replacement();
            ++p;
            valid_start = p;
        }
        else if (p + len > end)
        {
            /// Sequence was not fully written to this buffer.
            break;
        }
        else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(p), static_cast<int>(len)))
        {
            /// Valid sequence.
            p += len;
        }
        else
        {
            /// Invalid sequence. Skip just first byte.
            put_valid(valid_start, p - valid_start);
            put_replacement();
            ++p;
            valid_start = p;
        }
    }

    put_valid(valid_start, p - valid_start);

    if (p != end)
        put_replacement();
}

}
