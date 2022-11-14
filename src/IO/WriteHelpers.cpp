#include <IO/WriteHelpers.h>
#include <cinttypes>
#include <utility>
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

/** Further we want to generate constexpr array of strings with sizes from sequence of unsigned ints [0..N)
 *  in order to use this arrey for fast conversion of unsigned integers to strings
 */
namespace detail
{
    template <unsigned... digits>
    struct ToChars {
        static const char value[];
        static const size_t size;
     };

    template <unsigned... digits>
    constexpr char ToChars<digits...>::value[] = {('0' + digits)..., 0};

    template <unsigned... digits>
    constexpr size_t ToChars<digits...>::size = sizeof...(digits);

    template <unsigned rem, unsigned... digits>
    struct Decompose : Decompose<rem / 10, rem % 10, digits...> {};

    template <unsigned... digits>
    struct Decompose<0, digits...> : ToChars<digits...> {};

    template <>
    struct Decompose<0> : ToChars<0> {};

    template <unsigned num>
    struct NumToString : Decompose<num> {};

    template <class T, T... ints>
    constexpr std::array<std::pair<const char *, size_t>, sizeof...(ints)> str_make_array_impl(std::integer_sequence<T, ints...>)
    {
        return std::array<std::pair<const char *, size_t>, sizeof...(ints)> { std::pair<const char *, size_t> {NumToString<ints>::value, NumToString<ints>::size}... };
    }
}

/** str_make_array<N>() - generates static array of std::pair<const char *, size_t> for numbers [0..N), where:
 *      first - null-terminated string representing number
 *      second - size of the string as would returned by strlen()
 */
template <size_t N>
constexpr std::array<std::pair<const char *, size_t>, N> str_make_array()
{
    return detail::str_make_array_impl(std::make_integer_sequence<int, N>{});
}

/// This will generate static array of pair<const char *, size_t> for [0..255] at compile time
static constexpr auto byte_str = str_make_array<256>();

void writeIPv4Text(const IPv4 & ip, WriteBuffer & buf)
{
    size_t idx = (ip >> 24);
    buf.write(byte_str[idx].first, byte_str[idx].second);
    buf.write('.');
    idx = (ip >> 16) & 0xFF;
    buf.write(byte_str[idx].first, byte_str[idx].second);
    buf.write('.');
    idx = (ip >> 8) & 0xFF;
    buf.write(byte_str[idx].first, byte_str[idx].second);
    buf.write('.');
    idx = ip & 0xFF;
    buf.write(byte_str[idx].first, byte_str[idx].second);
}

void writeIPv6Text(const IPv6 & ip, WriteBuffer & buf)
{
    size_t shift = 120;
    for (int i = 0; i < 8; ++i)
    {
        size_t idx = (ip.toUnderType() >> shift) & 0xFF;
        buf.write(&hex_byte_to_char_lowercase_table[idx * 2], 2);
        shift -= 8;
        idx = (ip.toUnderType() >> shift) & 0xFF;
        buf.write(&hex_byte_to_char_lowercase_table[idx * 2], 2);
        if (shift == 0)
            break;
        shift -= 8;
        buf.write(':');
    }
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
