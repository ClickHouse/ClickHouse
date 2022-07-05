#include <DataTypes/Serializations/SerializationString.h>

#include <Columns/ColumnString.h>

#include <Common/MemorySanitizer.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <Core/Field.h>

#include <Formats/FormatSettings.h>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <cassert>
#include <microarchitecture/microarchitecture.h>

namespace DB
{

void SerializationString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void SerializationString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    String & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(s.data(), size);
}


void SerializationString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & s = assert_cast<const ColumnString &>(column).getDataAt(row_num);
    writeVarUInt(s.size, ostr);
    writeString(s, ostr);
}


void SerializationString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size;
    readVarUInt(size, istr);

    size_t old_chars_size = data.size();
    size_t offset = old_chars_size + size + 1;
    offsets.push_back(offset);

    try
    {
        data.resize(offset);
        istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
        data.back() = 0;
    }
    catch (...)
    {
        offsets.pop_back();
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void SerializationString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size ? offset + limit : size;

    if (offset == 0)
    {
        UInt64 str_size = offsets[0] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(data.data()), str_size);

        ++offset;
    }

    for (size_t i = offset; i < end; ++i)
    {
        UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
    }
}

#ifdef __x86_64__
#    define CH_DESERIALIZE_HAS_LARGE_MEMCPY
static const bool deserialize_large_memcpy = ::MicroArchitecture::runtimeHasERMS();
#elif defined(MICROARCHITECTURE_AARCH64_MOPS_ATTRIBUTE)
#    define CH_DESERIALIZE_HAS_LARGE_MEMCPY
static const bool deserialize_large_memcpy = ::MicroArchitecture::runtimeHasMOPS();
#endif

#ifndef ch_deserialize_inlined_memcpy
#    ifdef __clang__
#        define ch_deserialize_inlined_memcpy __builtin_memcpy_inline
#    elif defined(__GNUC__)
#        define ch_deserialize_inlined_memcpy __builtin_memcpy
#    else
#        define ch_deserialize_inlined_memcpy ::std::memcpy
#    endif
#endif

template <size_t BLOCK_SIZE>
ALWAYS_INLINE static inline void
deserializeBinaryBlockImpl(ColumnString::Chars & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
{
    size_t offset = data.size();
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size;
        readVarUInt(size, istr);

        offset += size + 1;
        offsets.push_back(offset);
        data.resize(offset);

#ifdef CH_DESERIALIZE_HAS_LARGE_MEMCPY
        if constexpr (BLOCK_SIZE >= 256)
        {
            /* The intel reference manual states that for sizes larger than 128, using REP MOVSB will give identical performance with other variants.
             * Beginning from 2048 bytes, REP MOVSB gives an even better performance.
             *
             * According to intel's reference manual:
             *
             * On older microarchitecture (ivy bridge), a REP MOVSB implementation of memcpy can achieve throughput at
             * slightly better than the 128-bit SIMD implementation when copying thousands of bytes.
             *
             * On newer microarchitecture (haswell), using REP MOVSB to implement memcpy operation for large copy length
             * can take advantage the 256-bit store data path and deliver throughput of more than 20 bytes per cycle.
             * For copy length that are smaller than a few hundred bytes, REP MOVSB approach is still slower than those
             * SIMD approaches.
             */
            if (size >= 4096 && deserialize_large_memcpy && istr.position() + size <= istr.buffer().end())
            {
                const auto * src = reinterpret_cast<const char *>(istr.position());
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);
                istr.position() += size;
                /*
                 *  For destination buffer misalignment:
                 *  The impact on Enhanced REP MOVSB and STOSB implementation can be 25%
                 *  degradation, while 128-bit AVX implementation of memcpy may degrade only
                 *  5%, relative to 16-byte aligned scenario.
                 *
                 *  Therefore, we manually align up the destination buffer before startup.
                 */
                ch_deserialize_inlined_memcpy(dst, src, 64);
                __msan_unpoison(dst, 64);
                auto address = reinterpret_cast<uintptr_t>(dst);
                auto shift = 64 - (address % 64);
                dst += shift;
                src += shift;
                size -= shift;
#    ifdef __x86_64__
                ::MicroArchitecture::repMovsb(dst, src, size);
#    elif defined(MICROARCHITECTURE_AARCH64_MOPS_ATTRIBUTE)
                ::MicroArchitecture::cpyf(dst, src, size);
#    endif
                __msan_unpoison(dst, size);
                data[offset - 1] = 0;
                continue;
            }
        }
#endif

        if (size)
        {
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + BLOCK_SIZE <= data.capacity() && istr.position() + size + BLOCK_SIZE <= istr.buffer().end())
            {
                const auto * src = reinterpret_cast<const char *>(istr.position());
                const auto * target = src + (size + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);
                while (src < target)
                {
                    /**
                     * Compiler expands the builtin memcpy perfectly. There is no need to
                     * manually write SSE2 code for x86 here; moreover, this method can also bring
                     * optimization to aarch64 targets.
                     *
                     * (x86 loop body for 64 bytes version with XMM registers)
                     *     movups  xmm0, xmmword ptr [rsi]
                     *     movups  xmm1, xmmword ptr [rsi + 16]
                     *     movups  xmm2, xmmword ptr [rsi + 32]
                     *     movups  xmm3, xmmword ptr [rsi + 48]
                     *     movups  xmmword ptr [rdi + 48], xmm3
                     *     movups  xmmword ptr [rdi + 32], xmm2
                     *     movups  xmmword ptr [rdi + 16], xmm1
                     *     movups  xmmword ptr [rdi], xmm0
                     *
                     * (aarch64 loop body for 64 bytes version with Q registers)
                     *     ldp     q0, q1, [x1]
                     *     ldp     q2, q3, [x1, #32]
                     *     stp     q0, q1, [x0]
                     *     add     x1, x1, #64
                     *     cmp     x1, x8
                     *     stp     q2, q3, [x0, #32]
                     *     add     x0, x0, #64
                     */
                    ch_deserialize_inlined_memcpy(dst, src, BLOCK_SIZE);
                    __msan_unpoison(dst, BLOCK_SIZE);
                    src += BLOCK_SIZE;
                    dst += BLOCK_SIZE;
                }
                assert(dst >= reinterpret_cast<decltype(dst)>(&data[offset - 1]));
                istr.position() += size;
            }
            else
            {
                istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
            }
        }

        data[offset - 1] = 0;
    }
}

#define DEFINE_DESERIALIZE_INSTANCE(SIZE) \
    MICROARCHITECTURE_DISPATCHED( \
        void, \
        deserializeBinaryBlock##SIZE, \
        (ColumnString::Chars & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit), \
        { return deserializeBinaryBlockImpl<SIZE>(data, offsets, istr, limit); })

DEFINE_DESERIALIZE_INSTANCE(16);
DEFINE_DESERIALIZE_INSTANCE(32);
DEFINE_DESERIALIZE_INSTANCE(48);
DEFINE_DESERIALIZE_INSTANCE(64);
DEFINE_DESERIALIZE_INSTANCE(128);
DEFINE_DESERIALIZE_INSTANCE(256);

void SerializationString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size = 1; /// By default reserve only for empty strings.

    if (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }

    size_t size_to_reserve = data.size() + std::ceil(limit * avg_chars_size);

    /// Never reserve for too big size.
    if (size_to_reserve < 256 * 1024 * 1024)
    {
        try
        {
            data.reserve(size_to_reserve);
        }
        catch (Exception & e)
        {
            e.addMessage(
                "(avg_value_size_hint = " + toString(avg_value_size_hint) + ", avg_chars_size = " + toString(avg_chars_size)
                + ", limit = " + toString(limit) + ")");
            throw;
        }
    }

    offsets.reserve(offsets.size() + limit);

    if (avg_chars_size >= 256)
        deserializeBinaryBlock256(data, offsets, istr, limit);
    else if (avg_chars_size >= 128)
        deserializeBinaryBlock128(data, offsets, istr, limit);
    else if (avg_chars_size >= 64)
        deserializeBinaryBlock64(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinaryBlock48(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinaryBlock32(data, offsets, istr, limit);
    else
        deserializeBinaryBlock16(data, offsets, istr, limit);
}


void SerializationString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void SerializationString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readStringUntilEOFInto(data, istr); });
}


void SerializationString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readEscapedStringInto(data, istr); });
}


void SerializationString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}


void SerializationString::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
}


void SerializationString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readJSONStringInto(data, istr); });
}


void SerializationString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString<>(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(column, [&](ColumnString::Chars & data) { readCSVStringInto(data, istr, settings.csv); });
}


}
