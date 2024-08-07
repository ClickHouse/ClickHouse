#include <Poco/UTF8Encoding.h>
#include <IO/WriteBufferValidUTF8.h>
#include <base/types.h>
#include <base/simd.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#      pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

namespace DB
{

const size_t WriteBufferValidUTF8::DEFAULT_SIZE = 4096;

/** Index into the table below with the first byte of a UTF-8 sequence to
  * get the number of trailing bytes that are supposed to follow it.
  * Note that *legal* UTF-8 values can't have 4 or 5-bytes. The table is
  * left as-is for anyone who may want to do such conversion, which was
  * allowed in earlier algorithms.
  */
extern const UInt8 length_of_utf8_sequence[256] =
{
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
    2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, 2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
    3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3, 4,4,4,4,4,4,4,4,5,5,5,5,6,6,6,6
};


WriteBufferValidUTF8::WriteBufferValidUTF8(
    WriteBuffer & output_buffer_, bool group_replacements_, const char * replacement_, size_t size)
    : BufferWithOwnMemory<WriteBuffer>(std::max(static_cast<size_t>(32), size)), output_buffer(output_buffer_),
    group_replacements(group_replacements_), replacement(replacement_)
{
}


inline void WriteBufferValidUTF8::putReplacement()
{
    if (replacement.empty() || (group_replacements && just_put_replacement))
        return;

    just_put_replacement = true;
    output_buffer.write(replacement.data(), replacement.size());
}


inline void WriteBufferValidUTF8::putValid(const char *data, size_t len)
{
    if (len == 0)
        return;

    just_put_replacement = false;
    output_buffer.write(data, len);
}


void WriteBufferValidUTF8::nextImpl()
{
    char * p = memory.data();
    char * valid_start = p;

    while (p < pos)
    {
#ifdef __SSE2__
        /// Fast skip of ASCII for x86.
        static constexpr size_t SIMD_BYTES = 16;
        const char * simd_end = p + (pos - p) / SIMD_BYTES * SIMD_BYTES;

        while (p < simd_end && !_mm_movemask_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p))))
            p += SIMD_BYTES;

        if (!(p < pos))
            break;
#elif defined(__aarch64__) && defined(__ARM_NEON)
        /// Fast skip of ASCII for aarch64.
        static constexpr size_t SIMD_BYTES = 16;
        const char * simd_end = p + (pos - p) / SIMD_BYTES * SIMD_BYTES;
        /// Other options include
        /// vmaxvq_u8(input) < 0b10000000;
        /// Used by SIMDJSON, has latency 3 for M1, 6 for everything else
        /// SIMDJSON uses it for 64 byte masks, so it's a little different.
        /// vmaxvq_u32(vandq_u32(input, vdupq_n_u32(0x80808080))) // u32 version has latency 3
        /// shrn version has universally <=3 cycles, on servers 2 cycles.
        while (p < simd_end && getNibbleMask(vcgeq_u8(vld1q_u8(reinterpret_cast<const uint8_t *>(p)), vdupq_n_u8(0x80))) == 0)
            p += SIMD_BYTES;

        if (!(p < pos))
            break;
#endif

        UInt8 len = length_of_utf8_sequence[static_cast<unsigned char>(*p)];

        if (len > 4)
        { // NOLINT
            /// Invalid start of sequence. Skip one byte.
            putValid(valid_start, p - valid_start);
            putReplacement();
            ++p;
            valid_start = p;
        }
        else if (p + len > pos)
        {
            /// Sequence was not fully written to this buffer.
            break;
        }
        else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<unsigned char *>(p), len))
        {
            /// Valid sequence.
            p += len;
        }
        else
        {
            /// Invalid sequence. Skip just first byte.
            putValid(valid_start, p - valid_start);
            putReplacement();
            ++p;
            valid_start = p;
        }
    }

    putValid(valid_start, p - valid_start);

    size_t cnt = pos - p;

    /// Shift unfinished sequence to start of buffer.
    for (size_t i = 0; i < cnt; ++i)
        memory[i] = p[i];

    working_buffer = Buffer(&memory[cnt], memory.data() + memory.size());
}

WriteBufferValidUTF8::~WriteBufferValidUTF8()
{
    finalize();
}

void WriteBufferValidUTF8::finalizeImpl()
{
    /// Write all complete sequences from buffer.
    nextImpl();

    /// Handle remaining bytes if we have an incomplete sequence
    if (working_buffer.begin() != memory.data())
    {
        const char * p = memory.data();

        while (p < pos)
        {
            UInt8 len = length_of_utf8_sequence[static_cast<const unsigned char>(*p)];
            if (p + len > pos)
            {
                /// Incomplete sequence. Skip one byte.
                putReplacement();
                ++p;
            }
            else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(p), len))
            {
                /// Valid sequence
                putValid(p, len);
                p += len;
            }
            else
            {
                /// Invalid sequence, skip first byte.
                putReplacement();
                ++p;
            }
        }
    }
}

}
