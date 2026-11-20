#include <IO/WriteHelpers.h>
#include <base/DecomposedFloat.h>
#include <base/hex.h>
#include <Common/formatIPv6.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wsign-compare"
#include <dragonbox/dragonbox_to_chars.h>
#pragma clang diagnostic pop

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}

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
    auto * dst_ptr = dst.data();

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    const auto * src_ptr = reinterpret_cast<const UInt8 *>(&uuid);
    const std::reverse_iterator<const UInt8 *> src(src_ptr + 16);
#else
    const auto * src = reinterpret_cast<const UInt8 *>(&uuid);
#endif
    formatHex(src + 8, dst_ptr, 4);
    dst[8] = '-';
    formatHex(src + 12, dst_ptr + 9, 2);
    dst[13] = '-';
    formatHex(src + 14, dst_ptr + 14, 2);
    dst[18] = '-';
    formatHex(src, dst_ptr + 19, 2);
    dst[23] = '-';
    formatHex(src + 2, dst_ptr + 24, 6);

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
    char addr[IPV6_MAX_TEXT_LENGTH] {};
    char * paddr = addr;

    formatIPv6(reinterpret_cast<const unsigned char *>(&ip), paddr);
    buf.write(addr, paddr - addr);
}

void writeException(const Exception & e, WriteBuffer & buf, bool with_stack_trace)
{
    writeBinaryLittleEndian(e.code(), buf);
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
static inline void writeProbablyQuotedStringImpl(std::string_view s, WriteBuffer & buf, F && write_quoted_string)
{
    static constexpr std::string_view distinct_str = "distinct";
    static constexpr std::string_view all_str = "all";
    static constexpr std::string_view table_str = "table";
    static constexpr std::string_view select_str = "select";
    if (isValidIdentifier(s)
        /// These are valid identifiers but are problematic if present unquoted in SQL query.
        && !(s.size() == distinct_str.size() && 0 == strncasecmp(s.data(), "distinct", s.size()))
        && !(s.size() == all_str.size() && 0 == strncasecmp(s.data(), "all", s.size()))
        && !(s.size() == table_str.size() && 0 == strncasecmp(s.data(), "table", s.size()))
        /// SELECT unquoted as an identifier would be re-parsed as the SELECT keyword and produce a
        /// different AST, e.g. arrayElement(Identifier("SELECT"), x) formats as SELECT[x], which
        /// re-parses as a subquery (SELECT [x]) with a different structure.
        && !(s.size() == select_str.size() && 0 == strncasecmp(s.data(), "select", s.size())))
    {
        writeString(s, buf);
    }
    else
        write_quoted_string(s, buf);
}

void writeProbablyBackQuotedString(std::string_view s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](std::string_view s_, WriteBuffer & buf_) { writeBackQuotedString(s_, buf_); });
}

void writeProbablyDoubleQuotedString(std::string_view s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](std::string_view s_, WriteBuffer & buf_) { writeDoubleQuotedString(s_, buf_); });
}

void writeProbablyBackQuotedStringMySQL(std::string_view s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](std::string_view s_, WriteBuffer & buf_) { writeBackQuotedStringMySQL(s_, buf_); });
}

void writePointerHex(const void * ptr, WriteBuffer & buf)
{
    writeString("0x", buf);
    char hex_str[2 * sizeof(ptr)];
    writeHexUIntLowercase(reinterpret_cast<uintptr_t>(ptr), hex_str);
    buf.write(hex_str, 2 * sizeof(ptr));
}

String fourSpaceIndent(size_t indent)
{
    return std::string(indent * 4, ' ');
}

template <typename T>
requires is_floating_point<T>
size_t writeFloatTextFastPath(T x, char * buffer)
{
    Int64 result = 0;

    if constexpr (std::is_same_v<T, Float64>)
    {
        /// The library dragonbox has low performance on integers.
        /// This workaround improves performance 6..10 times.

        if (DecomposedFloat64(x).isIntegerInRepresentableRange())
            result = itoa(Int64(x), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(x, buffer) - buffer;
    }
    else if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, BFloat16>)
    {
        Float32 f32 = Float32(x);
        if (DecomposedFloat32(f32).isIntegerInRepresentableRange())
            result = itoa(Int32(f32), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(f32, buffer) - buffer;
    }

    if (result <= 0)
        throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print floating point number");
    return result;
}

template size_t writeFloatTextFastPath(Float64 x, char * buffer);
template size_t writeFloatTextFastPath(Float32 x, char * buffer);
template size_t writeFloatTextFastPath(BFloat16 x, char * buffer);
}
