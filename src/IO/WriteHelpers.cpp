#include <IO/WriteHelpers.h>
#include <cinttypes>
#include <Common/hex.h>


namespace DB
{

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, size_t num_bytes)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; src_pos < num_bytes; ++src_pos)
    {
        writeHexByteLowercase(src[src_pos], &dst[dst_pos]);
        dst_pos += 2;
    }
}

void formatUUID(const UInt8 * src16, UInt8 * dst36)
{
    formatHex(&src16[0], &dst36[0], 4);
    dst36[8] = '-';
    formatHex(&src16[4], &dst36[9], 2);
    dst36[13] = '-';
    formatHex(&src16[6], &dst36[14], 2);
    dst36[18] = '-';
    formatHex(&src16[8], &dst36[19], 2);
    dst36[23] = '-';
    formatHex(&src16[10], &dst36[24], 6);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void formatUUID(std::reverse_iterator<const UInt8 *> src16, UInt8 * dst36)
{
    formatHex(src16 + 8, &dst36[0], 4);
    dst36[8] = '-';
    formatHex(src16 + 12, &dst36[9], 2);
    dst36[13] = '-';
    formatHex(src16 + 14, &dst36[14], 2);
    dst36[18] = '-';
    formatHex(src16, &dst36[19], 2);
    dst36[23] = '-';
    formatHex(src16 + 2, &dst36[24], 6);
}


void writeException(const Exception & e, WriteBuffer & buf, bool with_stack_trace)
{
    writeBinary(e.code(), buf);
    writeBinary(String(e.name()), buf);
    writeBinary(e.displayText() + getExtraExceptionInfo(e), buf);

    if (with_stack_trace)
        writeBinary(e.getStackTraceString(), buf);
    else
        writeBinary(String(), buf);

    bool has_nested = false;
    writeBinary(has_nested, buf);
}


/// The same, but quotes apply only if there are characters that do not match the identifier without quotes
template <typename F>
static inline void writeProbablyQuotedStringImpl(StringRef s, WriteBuffer & buf, F && write_quoted_string)
{
    if (isValidIdentifier(std::string_view{s})
        /// This are valid identifiers but are problematic if present unquoted in SQL query.
        && !(s.size == strlen("distinct") && 0 == strncasecmp(s.data, "distinct", strlen("distinct")))
        && !(s.size == strlen("all") && 0 == strncasecmp(s.data, "all", strlen("all"))))
    {
        writeString(s, buf);
    }
    else
        write_quoted_string(s, buf);
}

void writeProbablyBackQuotedString(StringRef s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](StringRef s_, WriteBuffer & buf_) { return writeBackQuotedString(s_, buf_); });
}

void writeProbablyDoubleQuotedString(StringRef s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](StringRef s_, WriteBuffer & buf_) { return writeDoubleQuotedString(s_, buf_); });
}

void writeProbablyBackQuotedStringMySQL(StringRef s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](StringRef s_, WriteBuffer & buf_) { return writeBackQuotedStringMySQL(s_, buf_); });
}

void writePointerHex(const void * ptr, WriteBuffer & buf)
{
    writeString("0x", buf);
    char hex_str[2 * sizeof(ptr)];
    writeHexUIntLowercase(reinterpret_cast<uintptr_t>(ptr), hex_str);
    buf.write(hex_str, 2 * sizeof(ptr));
}

}
