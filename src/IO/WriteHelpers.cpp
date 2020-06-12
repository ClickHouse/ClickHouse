#include <IO/WriteHelpers.h>
#include <inttypes.h>
#include <Common/hex.h>


namespace DB
{

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes)
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
    writeBinary(e.displayText(), buf);

    if (with_stack_trace)
        writeBinary(e.getStackTraceString(), buf);
    else
        writeBinary(String(), buf);

    bool has_nested = false;
    writeBinary(has_nested, buf);
}


/// The same, but quotes apply only if there are characters that do not match the identifier without quotes
template <typename F>
static inline void writeProbablyQuotedStringImpl(const StringRef & s, WriteBuffer & buf, F && write_quoted_string)
{
    if (!s.size || !isValidIdentifierBegin(s.data[0]))
    {
        write_quoted_string(s, buf);
    }
    else
    {
        const char * pos = s.data + 1;
        const char * end = s.data + s.size;
        for (; pos < end; ++pos)
            if (!isWordCharASCII(*pos))
                break;
        if (pos != end)
            write_quoted_string(s, buf);
        else
            writeString(s, buf);
    }
}

void writeProbablyBackQuotedString(const StringRef & s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const StringRef & s_, WriteBuffer & buf_) { return writeBackQuotedString(s_, buf_); });
}

void writeProbablyDoubleQuotedString(const StringRef & s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const StringRef & s_, WriteBuffer & buf_) { return writeDoubleQuotedString(s_, buf_); });
}

void writeProbablyBackQuotedStringMySQL(const StringRef & s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const StringRef & s_, WriteBuffer & buf_) { return writeBackQuotedStringMySQL(s_, buf_); });
}

}
