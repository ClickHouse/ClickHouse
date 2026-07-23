#include <IO/WriteHelpers.h>
#include <base/DecomposedFloat.h>
#include <base/hex.h>
#include <Common/formatIPv6.h>

#include <zmij.h>

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

namespace
{
/// When a floating-point number is an integer but its exponent exceeds the mantissa
/// width, the ULP (unit in the last place) is greater than 1. This means multiple
/// integers map to the same float. For example, Float32 with exp=25 has ULP=4, so
/// the floats are ..., 33554444, 33554448, 33554452, ...
///
/// A simple cast to integer (itoa) gives the exact value (e.g. 33554448), but
/// shortest-representation algorithms like dragonbox prefer "rounder" decimals
/// (e.g. 33554450) — values with more trailing zeros — as long as they round-trip
/// to the same float.
///
/// The two functions below (Float32 and Float64 variants) find the "roundest"
/// integer within the valid range centered on the exact value. They try powers
/// of 10 from largest to smallest, checking if a multiple of that power falls
/// within the range where any decimal rounds back to the same float.
///
/// The valid range is [exact - half_ulp_lower, exact + half_ulp_upper], where:
///
///   - For most floats, half_ulp_lower == half_ulp_upper == ULP/2.
///
///   - At power-of-2 boundaries (mantissa == 0), the previous float has a smaller
///     exponent, so the gap on the lower side is half the gap on the upper side.
///     Example: for 2^25 = 33554432 (Float32, exp=25, mantissa=0):
///       - Next float: 33554436 (gap = 4 = ULP)
///       - Previous float: 33554430 (gap = 2 = ULP/2, because it's in exp=24 territory)
///       - half_ulp_upper = 2, half_ulp_lower = 1
///
///   - IEEE 754 round-to-nearest-even tie-breaking: at the boundary midpoint, the
///     float with even mantissa "wins". Even mantissa → boundaries are inclusive;
///     odd mantissa → boundaries are exclusive.
///
/// The boundary condition is folded into the half_ulp values by subtracting 1
/// for odd mantissa. This turns "d < h || (d == h && inclusive)" into "d <= h_adj",
/// eliminating a parameter and multiple branches per power-of-10 check.
///
/// Try rounding v (positive) to the nearest multiple of compile-time constant p
/// that falls within [v - lo, v + hi]. If a candidate is found, writes it via
/// itoa to `buffer`, stores the total length (from `start`) in `result`, and
/// returns true. Returns false if neither the lower nor upper multiple is in range.
///
/// When rem == 0, v is already a multiple of p — down_ok is trivially true
/// (distance 0 <= lo), so the function returns v unchanged. No special case needed.
template <auto p, typename IntType>
ALWAYS_INLINE inline bool tryRoundToShortest(IntType v, IntType lo, IntType hi, char * buffer, const char * start, size_t & result)
{
    IntType rem = v % p;
    IntType d_up = p - rem;
    bool down_ok = rem <= lo;
    bool up_ok = d_up <= hi;
    if (down_ok | up_ok)
    {
        /// Prefer "up" when it's in range AND (down is not in range OR up is strictly closer).
        /// When equidistant (rem == d_up), prefer down (closer to zero).
        bool prefer_up = up_ok & (!down_ok | (d_up < rem));
        result = itoa(prefer_up ? v + d_up : v - rem, buffer) - start;
        return true;
    }
    return false;
}

/// Float32 variant: exp 25-30, shift = exp - 23 (2..7), half_ulp 2..64.
///   shift=2:    % 10 only
///   shift=3..6: % 100, fallback % 10
///   shift=7:    % 1000, fallback % 100
///
/// Verified: exhaustive check of all ~4.3 billion Float32 values (positive
/// and negative) confirms exact match with dragonbox output.
NO_INLINE size_t writeFloatTextFastPathFloat32Rounded(Float32 f32, int16_t exp, char * buffer)
{
    UInt32 bits;
    memcpy(&bits, &f32, sizeof(bits));
    UInt32 mantissa = bits & 0x7FFFFFu;

    Int32 shift = exp - 23;
    UInt32 half_ulp = UInt32(1) << (shift - 1);

    /// At power-of-2 boundaries (mantissa == 0), the previous float has a smaller
    /// exponent, so the lower half of the valid range is narrower.
    UInt32 half_ulp_lower = (mantissa == 0) ? (half_ulp >> 1) : half_ulp;

    /// Fold the inclusive/exclusive boundary into the half_ulp values:
    /// even mantissa → inclusive boundaries → no adjustment;
    /// odd mantissa → exclusive boundaries → subtract 1, turning < into <=.
    UInt32 adj = mantissa & 1;
    UInt32 lo = half_ulp_lower - adj;
    UInt32 hi = half_ulp - adj;

    /// Handle negative floats (e.g. BFloat16) by writing the sign, then
    /// rounding the absolute value.
    Int32 raw = Int32(f32);
    char * start = buffer;
    if (raw < 0)
    {
        *buffer++ = '-';
        raw = -raw;
    }
    UInt32 v = static_cast<UInt32>(raw);

    chassert(shift >= 2 && shift <= 7);

    size_t result;
    if (shift >= 7 && tryRoundToShortest<1000>(v, lo, hi, buffer, start, result)) return result;
    if (shift >= 3 && tryRoundToShortest<100>(v, lo, hi, buffer, start, result)) return result;
    if (tryRoundToShortest<10>(v, lo, hi, buffer, start, result)) return result;

    return itoa(v, buffer) - start;
}

/// Float64 variant: exp 54-62, shift = exp - 52 (2..10), half_ulp 2..512.
///   shift=2:     % 10 only
///   shift=3..6:  % 100, fallback % 10
///   shift=7..9:  % 1000, fallback % 100, fallback % 10
///   shift=10:    % 10000, fallback % 1000, fallback % 100, fallback % 10
///
/// Verified: 10 billion random Float64 samples (500M per exponent, exp 53-62,
/// both positive and negative) plus all power-of-2 boundaries and their
/// neighbors — zero mismatches with dragonbox. Exhaustive check is infeasible
/// for Float64 (2^62 values in range).
NO_INLINE size_t writeFloatTextFastPathFloat64Rounded(Float64 f64, int16_t exp, char * buffer)
{
    UInt64 bits;
    memcpy(&bits, &f64, sizeof(bits));
    UInt64 mantissa = bits & ((1ULL << 52) - 1);

    Int32 shift = exp - 52;
    UInt64 half_ulp = UInt64(1) << (shift - 1);

    /// At power-of-2 boundaries (mantissa == 0), the lower half of the valid range is narrower.
    UInt64 half_ulp_lower = (mantissa == 0) ? (half_ulp >> 1) : half_ulp;

    /// Fold the inclusive/exclusive boundary into the half_ulp values.
    UInt64 adj = mantissa & 1;
    UInt64 lo = half_ulp_lower - adj;
    UInt64 hi = half_ulp - adj;

    /// Handle negative values: write sign, then round the absolute value.
    Int64 raw = Int64(f64);
    char * start = buffer;
    if (raw < 0)
    {
        *buffer++ = '-';
        raw = -raw;
    }
    UInt64 v = static_cast<UInt64>(raw);

    chassert(shift >= 2 && shift <= 10);

    size_t result;
    if (shift >= 10 && tryRoundToShortest<10000>(v, lo, hi, buffer, start, result)) return result;
    if (shift >= 7 && tryRoundToShortest<1000>(v, lo, hi, buffer, start, result)) return result;
    if (shift >= 3 && tryRoundToShortest<100>(v, lo, hi, buffer, start, result)) return result;
    if (tryRoundToShortest<10>(v, lo, hi, buffer, start, result)) return result;

    return itoa(v, buffer) - start;
}

}

template <typename T>
requires is_floating_point<T>
size_t writeFloatTextFastPath(T x, char * buffer)
{
    if constexpr (std::is_same_v<T, Float64>)
    {
        DecomposedFloat64 decomposed(x);
        auto exp = decomposed.normalizedExponent();

        /// Float64 integer fast path, same structure as Float32:
        ///   exp 0..52:  exact integers (isIntegerInRepresentableRange).
        ///   exp 53:     ULP=2, all values are even integers, itoa matches dragonbox.
        ///   exp 54..62: ULP=4..1024, use rounding to find the "roundest" integer.
        ///   exp < 0:    |value| < 1, not an integer.
        ///   exp > 62:   |value| >= 2^63, overflows Int64.
        if (decomposed.isIntegerInRepresentableRange() || exp == 53)
            return itoa(Int64(x), buffer) - buffer;

        if (exp > 53 && exp <= 62)
            return writeFloatTextFastPathFloat64Rounded(x, exp, buffer);

        return zmij::detail::write(x, buffer) - buffer;
    }
    else if constexpr (std::is_same_v<T, Float32> || std::is_same_v<T, BFloat16>)
    {
        Float32 f32 = Float32(x);
        DecomposedFloat32 decomposed(f32);
        auto exp = decomposed.normalizedExponent();

        /// Float32 integer fast path: covers exp 0..30 (values up to ~2^31).
        ///
        /// Three sub-ranges:
        ///   exp 0..23:  ULP <= 1, every integer has a unique float. itoa gives the
        ///               exact value, which is the only valid shortest representation.
        ///               We just need to verify the float IS an integer (fractional
        ///               mantissa bits are zero).
        ///
        ///   exp 24:     ULP = 2, but cast-to-int always lands on an even number.
        ///               Exhaustive testing shows itoa matches dragonbox for all 8M values.
        ///
        ///   exp 25..30: ULP = 4..128. itoa gives the exact value, but dragonbox may
        ///               prefer a "rounder" decimal (more trailing zeros). We use
        ///               writeFloatTextFastPathFloat32Rounded to adjust, matching
        ///               dragonbox exactly for all ~4.3 billion Float32 values
        ///               (positive and negative, exhaustively verified).
        ///               zmij was patched to produce identical output to dragonbox,
        ///               so these results still hold.
        ///
        /// exp < 0:     |value| < 1, not an integer.
        /// exp > 30:    |value| >= 2^31, overflows Int32.

        /// Most common fast path first: exp 0..24, exact integers.
        if (decomposed.isIntegerInRepresentableRange() || exp == 24)
            return itoa(Int32(f32), buffer) - buffer;

        /// Extended fast path: exp 25..30, round to "roundest" decimal then itoa.
        if (exp > 24 && exp <= 30)
            return writeFloatTextFastPathFloat32Rounded(f32, exp, buffer);

        /// Not an integer, or exp out of range: use zmij.
        return zmij::detail::write(f32, buffer) - buffer;
    }
}

template size_t writeFloatTextFastPath(Float64 x, char * buffer);
template size_t writeFloatTextFastPath(Float32 x, char * buffer);
template size_t writeFloatTextFastPath(BFloat16 x, char * buffer);
}
