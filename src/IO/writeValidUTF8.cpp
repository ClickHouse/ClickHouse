#include <IO/writeValidUTF8.h>

#include "config.h"

#include <IO/WriteBuffer.h>
#include <Common/UTF8Helpers.h>

#include <base/types.h>

#if USE_SIMDUTF
#    include <simdutf.h>
#endif

#include <string_view>

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
        const UInt8 * u = reinterpret_cast<const UInt8 *>(p);

        /// Dense Unicode needs no ASCII scan.
        if (u[0] < 0x80)
        {
            p = skipASCII(begin, p, end);
            if (p == end)
                break;
            u = reinterpret_cast<const UInt8 *>(p);
        }

        /// A well-formed character moves `p` by a constant, so the next one does not wait for a table lookup.
        if (u[0] - 0xC2u < 0xE0u - 0xC2u)
        {
            if (end - p >= 2 && (u[1] & 0xC0) == 0x80)
            {
                p += 2;
                continue;
            }
        }
        else if (u[0] - 0xE0u < 0x10u)
        {
            if (end - p >= 3 && UTF8::isLegalSequence(u, 3))
            {
                p += 3;
                continue;
            }
        }
        else if (u[0] - 0xF0u < 0x05u)
        {
            if (end - p >= 4 && UTF8::isLegalSequence(u, 4))
            {
                p += 4;
                continue;
            }
        }

        /// Invalid or truncated sequences.
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
        else if (UTF8::isLegalSequence(reinterpret_cast<const UInt8 *>(p), len))
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
