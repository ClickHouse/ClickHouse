#include <IO/readFloatText.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <Core/Defines.h>
#include <base/shift10.h>
#include <Common/StringUtils.h>

#include <bit>
#include <cstring>
#include <limits>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#include <fast_float/fast_float.h>
#pragma clang diagnostic pop

/// The analyzer chases fast_float's flag-enum operators (chars_format) and ascii number parsing
/// through every call site below, so the suppression spans the whole file.
// NOLINTBEGIN(clang-analyzer-core.UndefinedBinaryOperatorResult,clang-analyzer-optin.core.EnumCastOutOfRange)

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

/** Must successfully parse inf, INF and Infinity.
  * All other variants in different cases are also parsed for simplicity.
  */
bool parseInfinity(ReadBuffer & buf)
{
    if (!checkStringCaseInsensitive("inf", buf))
        return false;

    /// Just inf.
    if (buf.eof() || !isWordCharASCII(*buf.position()))
        return true;

    /// If word characters after inf, it should be infinity.
    return checkStringCaseInsensitive("inity", buf);
}


/** Must successfully parse nan, NAN and NaN.
  * All other variants in different cases are also parsed for simplicity.
  */
bool parseNaN(ReadBuffer & buf)
{
    return checkStringCaseInsensitive("nan", buf);
}


void assertInfinity(ReadBuffer & buf)
{
    if (!parseInfinity(buf))
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot parse infinity.");
}

void assertNaN(ReadBuffer & buf)
{
    if (!parseNaN(buf))
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot parse NaN.");
}


namespace
{

template <bool throw_exception>
bool assertOrParseInfinity(ReadBuffer & buf)
{
    if constexpr (throw_exception)
    {
        assertInfinity(buf);
        return true;
    }
    else
        return parseInfinity(buf);
}

template <bool throw_exception>
bool assertOrParseNaN(ReadBuffer & buf)
{
    if constexpr (throw_exception)
    {
        assertNaN(buf);
        return true;
    }
    else
        return parseNaN(buf);
}


// credit: https://johnnylee-sde.github.io/Fast-numeric-string-to-int/
inline bool is_made_of_eight_digits_fast(uint64_t val) noexcept
{
    return (((val & 0xF0F0F0F0F0F0F0F0) | (((val + 0x0606060606060606) & 0xF0F0F0F0F0F0F0F0) >> 4)) == 0x3333333333333333);
}

inline bool is_made_of_eight_digits_fast(const char * chars) noexcept
{
    uint64_t val = 0;
    ::memcpy(&val, chars, 8);
    return is_made_of_eight_digits_fast(val);
}

/// Convert 8 ASCII decimal digits (read as a little-endian uint64) into their integer value. credit: @aqrit
inline uint32_t parse_eight_digits_unrolled(uint64_t val) noexcept
{
    const uint64_t mask = 0x000000FF000000FF;
    const uint64_t mul1 = 0x000F424000000064; // 100 + (1000000ULL << 32)
    const uint64_t mul2 = 0x0000271000000001; // 1 + (10000ULL << 32)
    val -= 0x3030303030303030;
    val = (val * 10) + (val >> 8);
    val = (((val & mask) * mul1) + (((val >> 16) & mask) * mul2)) >> 32;
    return static_cast<uint32_t>(val);
}

/// A pure decimal integer of up to 38 digits fits exactly in unsigned __int128, and the conversion
/// to double/float is correctly rounded. That is far cheaper than fast_float's big-integer path for
/// long inputs (e.g. 19-20 digit integers), and exactly as correct.
/// Returns true (and sets @x and @parse_end) on a pure integer; false to fall back otherwise
/// (decimal point, exponent, sign issues, or more than 38 digits).
constexpr int max_u128_integer_digits = 38;

/// A value of at most this many characters has at most this many significant digits, so
/// fast_float's default from_chars (store_spans=false) never hits the too_many_digits double-parse
/// and is fastest. Longer inputs use the unsigned __int128 integer path or fromCharsLong.
constexpr ptrdiff_t max_short_float_chars = 19;

template <typename T>
inline bool tryReadLongIntegerToFloat(T & x, const char * first, const char * last, const char *& parse_end)
{
    const char * p = first;
    bool negative = false;
    if (p < last && (*p == '-' || *p == '+'))
    {
        negative = (*p == '-');
        ++p;
    }

    const char * const digits_begin = p;
    unsigned __int128 value = 0;
    /// Accumulate 8 digits at a time via SWAR (byteswap on big-endian; see parse_eight_digits_unrolled).
    while (p + 8 <= last && is_made_of_eight_digits_fast(p))
    {
        uint64_t chunk = 0;
        ::memcpy(&chunk, p, 8);
        if constexpr (std::endian::native == std::endian::big)
            chunk = std::byteswap(chunk);
        value = value * 100000000ULL + parse_eight_digits_unrolled(chunk);
        p += 8;
    }
    while (p < last && isNumericASCII(*p))
    {
        value = value * 10 + static_cast<unsigned>(*p - '0');
        ++p;
    }

    const auto num_digits = p - digits_begin;
    /// Not a pure integer (fraction/exponent follows), empty, or too many digits to fit exactly.
    if (num_digits == 0 || num_digits > max_u128_integer_digits)
        return false;
    if (p < last && (*p == '.' || *p == 'e' || *p == 'E'))
        return false;

    auto result = static_cast<T>(value);
    x = negative ? -result : result;
    parse_end = p;
    return true;
}


/// Correctly-rounded parse of a pure decimal integer with MORE than 38 significant digits (does not
/// fit in unsigned __int128). Such an input goes through fast_float's big-integer path, but
/// fast_float wastefully accumulates every digit and then recomputes the mantissa from the first 19.
/// Instead, build the parsed_number_string_t ourselves: accumulate only the first 19 significant
/// digits, then SWAR-skip (count) the rest, and hand the result to fast_float::from_chars_advanced,
/// which keeps the proven Eisel-Lemire + digit_comp rounding. Returns false (fall back) for
/// non-integers. This mirrors parse_number_string's too_many_digits semantics for an integer.
template <typename T>
inline bool tryReadBigIntegerToFloat(T & x, const char * first, const char * last, fast_float::from_chars_result_t<char> & result)
{
    const char * p = first;
    bool negative = false;
    if (p < last && (*p == '-' || *p == '+'))
    {
        negative = (*p == '-');
        ++p;
    }

    const char * const integer_begin = p;
    while (p < last && *p == '0') /// leading zeros are part of the integer span but not significant
        ++p;

    uint64_t mantissa = 0;
    int significant = 0;
    while (p < last && significant < 19 && isNumericASCII(*p))
    {
        mantissa = mantissa * 10 + static_cast<unsigned>(*p - '0');
        ++p;
        ++significant;
    }

    /// Count (do not accumulate) the remaining significant digits, 8 at a time.
    const char * const rest_begin = p;
    while (p + 8 <= last && is_made_of_eight_digits_fast(p))
        p += 8;
    while (p < last && isNumericASCII(*p))
        ++p;

    /// Pure integer only; a fraction/exponent must go through the general parser.
    if (p < last && (*p == '.' || *p == 'e' || *p == 'E'))
        return false;

    const auto total_significant = significant + (p - rest_begin);
    if (total_significant <= max_u128_integer_digits) /// handled by the exact unsigned __int128 path
        return false;

    fast_float::parsed_number_string_t<char> pns;
    pns.valid = true;
    pns.too_many_digits = true;
    pns.negative = negative;
    pns.mantissa = mantissa;                       /// first 19 significant digits, truncated
    pns.exponent = total_significant - 19;         /// integer scaling, matching parse_number_string
    pns.lastmatch = p;
    pns.integer = fast_float::span<const char>(integer_begin, static_cast<size_t>(p - integer_begin));
    pns.fraction = fast_float::span<const char>(p, 0);

    /// Return the backend result directly, including result_out_of_range (e.g. a >38-digit
    /// integer that overflows Float32) — falling back to fromCharsLong would just re-scan and
    /// reach the same error more slowly.
    result = fast_float::from_chars_advanced(pns, x);
    return true;
}


/// Variant of fast_float::from_chars tuned for inputs known to have many significant digits.
/// Built on fast_float's existing primitives (no fork of the library): parse once with the
/// integer/fraction spans materialized (store_spans=true) and run the full algorithm, so the
/// >19-significant-digit case is handled in one pass instead of from_chars's store_spans=false
/// hot path re-parsing the whole string.
template <typename T>
fast_float::from_chars_result_t<char>
fromCharsLong(const char * first, const char * last, T & value, fast_float::chars_format fmt)
{
    const fast_float::parse_options_t<char> options(fmt);
    const fast_float::chars_format adjusted = fast_float::detail::adjust_for_feature_macros(fmt);

    auto pns = fast_float::parse_number_string<false, char>(first, last, options, /*store_spans=*/true);
    if (!pns.valid)
    {
        if (uint64_t(adjusted & fast_float::chars_format::no_infnan))
        {
            fast_float::from_chars_result_t<char> answer{};
            answer.ec = std::errc::invalid_argument;
            answer.ptr = first;
            return answer;
        }
        return fast_float::detail::parse_infnan(first, last, value, adjusted);
    }
    return fast_float::from_chars_advanced(pns, value);
}


/// Length, capped just past the 38-digit boundary, of the leading integer run at [first, last) --
/// i.e. the size of the integer token, NOT of the whole remaining buffer. The dispatch below must
/// classify by the token: a short token like "1" sitting in a large buffer (e.g. "1.5 GiB", or any
/// non-exact ReadBuffer) must still take the fast from_chars path. A '.', 'e' or 'E' right after the
/// digits means it is a fraction/exponent, not an integer, so we return max_short_float_chars to send
/// it to from_chars directly. The cap is enough to tell short / <=38-digit / longer integers apart;
/// the chosen parser still receives the real `last` and consumes the full token.
inline ptrdiff_t floatTokenClassLength(const char * first, const char * last)
{
    const char * const scan_end = (last - first > max_u128_integer_digits + 2) ? first + (max_u128_integer_digits + 2) : last;
    const char * p = first;
    if (p < scan_end && (*p == '-' || *p == '+'))
        ++p;
    while (p + 8 <= scan_end && is_made_of_eight_digits_fast(p)) /// skip digit runs 8 at a time
        p += 8;
    while (p < scan_end && isNumericASCII(*p))
        ++p;
    /// A '.', 'e' or 'E' means this is not a pure integer, so neither integer fast path applies.
    /// Classify it as short so the dispatch hands it to from_chars for a single-pass parse.
    if (p < scan_end && (*p == '.' || *p == 'e' || *p == 'E'))
        return max_short_float_chars;
    return p - first;
}

/// The whole length-based dispatch for parsing a float from a contiguous [first, last) range,
/// shared by the no-copy and copy paths of readFloatTextPreciseImpl (inlined). Picks:
///   - short token (<= max_short_float_chars)         -> fast_float::from_chars,
///   - long pure integer (<= 38 digits)              -> unsigned __int128 (correctly rounded, cheap),
///   - otherwise (long fraction / huge integer)      -> fromCharsLong (single parse).
/// On big-endian, fast_float parsing directly to float can misbehave, so parse as double and
/// narrow (the unsigned __int128 path is endian-independent and needs no such workaround).
template <typename T>
inline fast_float::from_chars_result_t<char>
parseFloatFromRange(T & x, const char * first, const char * last, fast_float::chars_format fmt)
{
    /// Classify by the token length, but only scan when the buffer holds more than a short token's
    /// worth of bytes (so short inputs cost nothing extra).
    const auto length = (last - first <= max_short_float_chars) ? (last - first) : floatTokenClassLength(first, last);

    if (length > max_short_float_chars)
    {
        /// <= 38-digit integer: exact via unsigned __int128 (always in range, so a plain success).
        if (length <= max_u128_integer_digits + 1)
        {
            const char * int_end = nullptr;
            if (tryReadLongIntegerToFloat(x, first, last, int_end))
            {
                fast_float::from_chars_result_t<char> answer{};
                answer.ptr = int_end;
                answer.ec = std::errc();
                return answer;
            }
        }
        /// Longer integer: build the parsed_number_string_t directly (lean wide scan) and use
        /// fast_float's rounding backend; its result (incl. out-of-range) is returned as-is.
        else
        {
            fast_float::from_chars_result_t<char> answer{};
            if (tryReadBigIntegerToFloat(x, first, last, answer))
                return answer;
        }
    }

    const bool is_short = length <= max_short_float_chars;
    if constexpr (std::endian::native == std::endian::little)
        return is_short ? fast_float::from_chars(first, last, x, fmt)
                        : fromCharsLong(first, last, x, fmt);
    else
    {
        Float64 wide = 0.0;
        auto answer = is_short ? fast_float::from_chars(first, last, wide, fmt)
                               : fromCharsLong(first, last, wide, fmt);
        x = static_cast<T>(wide);
        return answer;
    }
}


template <typename T, typename ReturnType>
ReturnType readFloatTextPreciseImpl(T & x, ReadBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextPreciseImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII");

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    static constexpr int MAX_LENGTH = 316;
    static constexpr auto float_fmt = fast_float::chars_format::general | fast_float::chars_format::allow_leading_plus;

    /// Fast path (avoid copying) if the buffer has at least MAX_LENGTH bytes or the whole input is in memory.
    /// isMemoryBuffer() is a cheap virtual call, replacing a per-value dynamic_cast.
    if (likely(!buf.eof() && (buf.isMemoryBuffer() || buf.position() + MAX_LENGTH <= buf.buffer().end())))
    {
        auto * initial_position = buf.position();
        auto * const buf_end = buf.buffer().end();
        auto res = parseFloatFromRange(x, initial_position, buf_end, float_fmt);

        if (unlikely(res.ec != std::errc()))
        {
            if constexpr (throw_exception)
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_NUMBER,
                    "Cannot read floating point value here: {}",
                    String(initial_position, buf.buffer().end() - initial_position));
            else
                return ReturnType(false);
        }

        buf.position() += res.ptr - initial_position;

        return ReturnType(true);
    }

    /// Slow path. Copy characters that may be present in floating point number to temporary buffer.
    bool negative = false;

    /// We check eof here because we can parse +inf +nan
    while (!buf.eof())
    {
        switch (*buf.position())
        {
            case '+':
                ++buf.position();
                continue;

            case '-':
            {
                negative = true;
                ++buf.position();
                continue;
            }

            case 'i': [[fallthrough]];
            case 'I':
            {
                if (assertOrParseInfinity<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::infinity();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            case 'n': [[fallthrough]];
            case 'N':
            {
                if (assertOrParseNaN<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::quiet_NaN();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            default:
                break;
        }

        break;
    }

    char tmp_buf[MAX_LENGTH];
    int num_copied_chars = 0;

    while (!buf.eof() && num_copied_chars < MAX_LENGTH)
    {
        char c = *buf.position();
        if (!(isNumericASCII(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'))
            break;

        tmp_buf[num_copied_chars] = c;
        ++buf.position();
        ++num_copied_chars;
    }

    /// Sign was already consumed above (tracked in `negative`), so tmp_buf holds no leading sign.
    auto res = parseFloatFromRange(x, tmp_buf, tmp_buf + num_copied_chars, fast_float::chars_format::general);
    if (unlikely(res.ec != std::errc() || res.ptr - tmp_buf != num_copied_chars))
    {
        if constexpr (throw_exception)
            throw Exception(
                ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value here: {}", String(tmp_buf, num_copied_chars));
        else
            return ReturnType(false);
    }

    if (negative)
        x = -x;

    return ReturnType(true);
}


template <size_t N, typename T>
inline void readUIntTextUpToNSignificantDigits(T & x, ReadBuffer & buf)
{
    /// In optimistic case we can skip bound checking for first loop.
    if (buf.position() + N <= buf.buffer().end())
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                return;
        }
    }
    else
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (!buf.eof() && isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                return;
        }
    }

    while (!buf.eof() && (buf.position() + 8 <= buf.buffer().end()) && is_made_of_eight_digits_fast(buf.position()))
        buf.position() += 8;

    while (!buf.eof() && isNumericASCII(*buf.position()))
        ++buf.position();
}


template <typename T, typename ReturnType, bool allow_exponent = true>
ReturnType readFloatTextFastImpl(T & x, ReadBuffer & in, bool & has_fractional)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII");

    has_fractional = false;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    x = 0;
    UInt64 before_point = 0;
    UInt64 after_point = 0;
    int after_point_exponent = 0;
    int exponent = 0;

    if (in.eof())
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value");
        else
            return false;
    }

    if (*in.position() == '-')
    {
        negative = true;
        ++in.position();
    }
    else if (*in.position() == '+')
        ++in.position();

    auto count_after_sign = in.count();

    constexpr int significant_digits = std::numeric_limits<UInt64>::digits10;
    readUIntTextUpToNSignificantDigits<significant_digits>(before_point, in);

    size_t read_digits = in.count() - count_after_sign;

    if (unlikely(read_digits > significant_digits))
    {
        int before_point_additional_exponent = static_cast<int>(read_digits) - significant_digits;
        x = static_cast<T>(shift10(before_point, before_point_additional_exponent));
    }
    else
    {
        x = static_cast<T>(before_point);

        /// Shortcut for the common case when there is an integer that fit in Int64.
        if (read_digits && (in.eof() || *in.position() < '.'))
        {
            if (negative)
                x = -x;
            return ReturnType(true);
        }
    }

    if (checkChar('.', in))
    {
        has_fractional = true;
        auto after_point_count = in.count();

        while (!in.eof() && *in.position() == '0')
            ++in.position();

        auto after_leading_zeros_count = in.count();
        int after_point_num_leading_zeros = static_cast<int>(after_leading_zeros_count - after_point_count);

        readUIntTextUpToNSignificantDigits<significant_digits>(after_point, in);
        read_digits = in.count() - after_leading_zeros_count;
        after_point_exponent = (read_digits > significant_digits ? -significant_digits : static_cast<int>(-read_digits)) - after_point_num_leading_zeros;
    }

    if constexpr (allow_exponent)
    {
        if (checkChar('e', in) || checkChar('E', in))
        {
            has_fractional = true;
            if (in.eof())
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value: nothing after exponent");
                else
                    return false;
            }

            bool exponent_negative = false;
            if (*in.position() == '-')
            {
                exponent_negative = true;
                ++in.position();
            }
            else if (*in.position() == '+')
            {
                ++in.position();
            }

            readUIntTextUpToNSignificantDigits<4>(exponent, in);
            if (exponent_negative)
                exponent = -exponent;
        }
    }

    if (after_point)
    {
        x += static_cast<T>(shift10(after_point, after_point_exponent));
    }

    if (exponent)
    {
        x = static_cast<T>(shift10(x, exponent));
    }

    if (negative)
        x = -x;

    auto num_characters_without_sign = in.count() - count_after_sign;

    /// Denormals. At most one character is read before denormal and it is '-'.
    if (num_characters_without_sign == 0)
    {
        if (in.eof())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value: no digits read");
            else
                return false;
        }

        if (*in.position() == '+')
        {
            ++in.position();
            if (in.eof())
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value: nothing after plus sign");
                else
                    return false;
            }
            else if (negative)
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot read floating point value: plus after minus sign");
                else
                    return false;
            }
        }

        if (*in.position() == 'i' || *in.position() == 'I')
        {
            if (assertOrParseInfinity<throw_exception>(in))
            {
                x = std::numeric_limits<T>::infinity();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
        if (*in.position() == 'n' || *in.position() == 'N')
        {
            if (assertOrParseNaN<throw_exception>(in))
            {
                x = std::numeric_limits<T>::quiet_NaN();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
    }

    return ReturnType(true);
}

template <typename T, typename ReturnType>
ReturnType readFloatTextSimpleImpl(T & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    x = 0;
    bool after_point = false;
    T power_of_ten = 1;

    if (buf.eof())
        throwReadAfterEOF();

    while (!buf.eof())
    {
        switch (*buf.position())
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '.':
                after_point = true;
                break;
            case '0': [[fallthrough]];
            case '1': [[fallthrough]];
            case '2': [[fallthrough]];
            case '3': [[fallthrough]];
            case '4': [[fallthrough]];
            case '5': [[fallthrough]];
            case '6': [[fallthrough]];
            case '7': [[fallthrough]];
            case '8': [[fallthrough]];
            case '9':
                if (after_point)
                {
                    power_of_ten /= 10;
                    x += (*buf.position() - '0') * power_of_ten;
                }
                else
                {
                    x *= 10;
                    x += *buf.position() - '0';
                }
                break;
            case 'e': [[fallthrough]];
            case 'E':
            {
                ++buf.position();
                Int32 exponent = 0;
                readIntText(exponent, buf);
                x = shift10(x, exponent);
                if (negative)
                    x = -x;
                return ReturnType(true);
            }

            case 'i': [[fallthrough]];
            case 'I':
            {
                if (assertOrParseInfinity<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::infinity();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            case 'n': [[fallthrough]];
            case 'N':
            {
                if (assertOrParseNaN<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::quiet_NaN();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            default:
            {
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
        }
        ++buf.position();
    }

    if (negative)
        x = -x;

    return ReturnType(true);
}

}


template <typename T> void readFloatTextPrecise(T & x, ReadBuffer & in)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        readFloatTextPreciseImpl<Float32, void>(tmp, in);
        x = BFloat16(tmp);
    }
    else
        readFloatTextPreciseImpl<T, void>(x, in);
}

template <typename T> bool tryReadFloatTextPrecise(T & x, ReadBuffer & in)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextPreciseImpl<Float32, bool>(tmp, in);
        if (res)
            x = BFloat16(tmp);
        return res;
    }
    else
        return readFloatTextPreciseImpl<T, bool>(x, in);
}

template <typename T> void readFloatTextFast(T & x, ReadBuffer & in)
{
    bool has_fractional = false;
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        readFloatTextFastImpl<Float32, void>(tmp, in, has_fractional);
        x = BFloat16(tmp);
    }
    else
        readFloatTextFastImpl<T, void>(x, in, has_fractional);
}

template <typename T> bool tryReadFloatTextFast(T & x, ReadBuffer & in)
{
    bool has_fractional = false;
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextFastImpl<Float32, bool>(tmp, in, has_fractional);
        if (res)
            x = BFloat16(tmp);
        return res;
    }
    else
        return readFloatTextFastImpl<T, bool>(x, in, has_fractional);
}

template <typename T> void readFloatTextSimple(T & x, ReadBuffer & in)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        readFloatTextSimpleImpl<Float32, void>(tmp, in);
        x = BFloat16(tmp);
    }
    else
        readFloatTextSimpleImpl<T, void>(x, in);
}

template <typename T> bool tryReadFloatTextSimple(T & x, ReadBuffer & in)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextSimpleImpl<Float32, bool>(tmp, in);
        if (res)
            x = BFloat16(tmp);
        return res;
    }
    else
        return readFloatTextSimpleImpl<T, bool>(x, in);
}


template <typename T> void readFloatText(T & x, ReadBuffer & in) { readFloatTextFast(x, in); }
template <typename T> bool tryReadFloatText(T & x, ReadBuffer & in) { return tryReadFloatTextFast(x, in); }

template <typename T> bool tryReadFloatTextNoExponent(T & x, ReadBuffer & in)
{
    bool has_fractional = false;
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextFastImpl<Float32, bool, false>(tmp, in, has_fractional);
        if (res)
            x = BFloat16(tmp);
        return res;

    }
    else
        return readFloatTextFastImpl<T, bool, false>(x, in, has_fractional);
}

template <typename T> bool tryReadFloatTextExt(T & x, ReadBuffer & in, bool & has_fractional)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextFastImpl<Float32, bool>(tmp, in, has_fractional);
        if (res)
            x = BFloat16(tmp);
        return res;
    }
    else
        return readFloatTextFastImpl<T, bool>(x, in, has_fractional);
}

template <typename T> bool tryReadFloatTextExtNoExponent(T & x, ReadBuffer & in, bool & has_fractional)
{
    if constexpr (std::is_same_v<T, BFloat16>)
    {
        Float32 tmp = 0;
        bool res = readFloatTextFastImpl<Float32, bool, false>(tmp, in, has_fractional);
        if (res)
            x = BFloat16(tmp);
        return res;
    }
    else
        return readFloatTextFastImpl<T, bool, false>(x, in, has_fractional);
}


template void readFloatTextPrecise<BFloat16>(BFloat16 &, ReadBuffer &);
template void readFloatTextPrecise<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextPrecise<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextPrecise<BFloat16>(BFloat16 &, ReadBuffer &);
template bool tryReadFloatTextPrecise<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextPrecise<Float64>(Float64 &, ReadBuffer &);

template void readFloatTextFast<BFloat16>(BFloat16 &, ReadBuffer &);
template void readFloatTextFast<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextFast<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextFast<BFloat16>(BFloat16 &, ReadBuffer &);
template bool tryReadFloatTextFast<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextFast<Float64>(Float64 &, ReadBuffer &);

template void readFloatTextSimple<BFloat16>(BFloat16 &, ReadBuffer &);
template void readFloatTextSimple<Float32>(Float32 &, ReadBuffer &);
template void readFloatTextSimple<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatTextSimple<BFloat16>(BFloat16 &, ReadBuffer &);
template bool tryReadFloatTextSimple<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextSimple<Float64>(Float64 &, ReadBuffer &);

template void readFloatText<BFloat16>(BFloat16 &, ReadBuffer &);
template void readFloatText<Float32>(Float32 &, ReadBuffer &);
template void readFloatText<Float64>(Float64 &, ReadBuffer &);
template bool tryReadFloatText<BFloat16>(BFloat16 &, ReadBuffer &);
template bool tryReadFloatText<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatText<Float64>(Float64 &, ReadBuffer &);

template bool tryReadFloatTextNoExponent<BFloat16>(BFloat16 &, ReadBuffer &);
template bool tryReadFloatTextNoExponent<Float32>(Float32 &, ReadBuffer &);
template bool tryReadFloatTextNoExponent<Float64>(Float64 &, ReadBuffer &);

template bool tryReadFloatTextExt<BFloat16>(BFloat16 &, ReadBuffer &, bool &);
template bool tryReadFloatTextExt<Float32>(Float32 &, ReadBuffer &, bool &);
template bool tryReadFloatTextExt<Float64>(Float64 &, ReadBuffer &, bool &);
template bool tryReadFloatTextExtNoExponent<BFloat16>(BFloat16 &, ReadBuffer &, bool &);
template bool tryReadFloatTextExtNoExponent<Float32>(Float32 &, ReadBuffer &, bool &);
template bool tryReadFloatTextExtNoExponent<Float64>(Float64 &, ReadBuffer &, bool &);

}

// NOLINTEND(clang-analyzer-core.UndefinedBinaryOperatorResult,clang-analyzer-optin.core.EnumCastOutOfRange)
