#include <IO/WriteHelpers.h>
#include <cinttypes>
#include <utility>
#include <Common/formatIPv6.h>
#include <base/hex.h>


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

std::array<char, 36> formatUUID(const UUID & uuid)
{
    std::array<char, 36> dst;
    const auto * src_ptr = reinterpret_cast<const UInt8 *>(&uuid);
    auto * dst_ptr = dst.data();
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    const std::reverse_iterator src_it(src_ptr + 16);
    formatHex(src_it + 8, dst_ptr, 4);
    dst[8] = '-';
    formatHex(src_it + 12, dst_ptr + 9, 2);
    dst[13] = '-';
    formatHex(src_it + 14, dst_ptr + 14, 2);
    dst[18] = '-';
    formatHex(src_it, dst_ptr + 19, 2);
    dst[23] = '-';
    formatHex(src_it + 2, dst_ptr + 24, 6);
#else
    formatHex(src_ptr, dst_ptr, 4);
    dst[8] = '-';
    formatHex(src_ptr + 4, dst_ptr + 9, 2);
    dst[13] = '-';
    formatHex(src_ptr + 6, dst_ptr + 14, 2);
    dst[18] = '-';
    formatHex(src_ptr + 8, dst_ptr + 19, 2);
    dst[23] = '-';
    formatHex(src_ptr + 10, dst_ptr + 24, 6);
#endif

    return dst;
}

void writeIPv4Text(const IPv4 & ip, WriteBuffer & buf)
{
    size_t idx = (ip >> 24);
    buf.write(one_byte_to_string_lookup_table[idx].first, one_byte_to_string_lookup_table[idx].second);
    buf.write('.');
    idx = (ip >> 16) & 0xFF;
    buf.write(one_byte_to_string_lookup_table[idx].first, one_byte_to_string_lookup_table[idx].second);
    buf.write('.');
    idx = (ip >> 8) & 0xFF;
    buf.write(one_byte_to_string_lookup_table[idx].first, one_byte_to_string_lookup_table[idx].second);
    buf.write('.');
    idx = ip & 0xFF;
    buf.write(one_byte_to_string_lookup_table[idx].first, one_byte_to_string_lookup_table[idx].second);
}

void writeIPv6Text(const IPv6 & ip, WriteBuffer & buf)
{
    char addr[IPV6_MAX_TEXT_LENGTH + 1] {};
    char * paddr = addr;

    formatIPv6(reinterpret_cast<const unsigned char *>(&ip), paddr);
    buf.write(addr, paddr - addr - 1);
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
    if (isValidIdentifier(s.toView())
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
