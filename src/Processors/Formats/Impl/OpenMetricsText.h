#pragma once

#include <base/types.h>
#include <base/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

#include <cstddef>
#include <limits>
#include <string>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

/// Contract shared by the OpenMetrics text reader (OpenMetricsTextRowInputFormat) and writer
/// (OpenMetricsTextOutputFormat). Both sides must agree on the metric/label name grammar, the
/// label-value escape set, the millisecond<->seconds timestamp round-trip, and the OpenMetrics 1.0
/// metric-type vocabulary, so the single source of truth lives here to keep read and write from
/// drifting.
namespace OpenMetricsText
{

inline constexpr const char * FORMAT_NAME = "OpenMetrics";

[[noreturn]] inline void throwIncorrect(std::string_view what, std::string_view line)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{} in OpenMetrics line: {}", what, line);
}

/// `[a-zA-Z_:][a-zA-Z0-9_:]*` if allow_colon (metric names), else `[a-zA-Z_][a-zA-Z0-9_]*` (label names).
inline bool isValidName(std::string_view name, bool allow_colon)
{
    const auto ok = [allow_colon](char c, bool first)
    {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_')
            return true;
        if (allow_colon && c == ':')
            return true;
        return !first && c >= '0' && c <= '9';
    };
    if (name.empty() || !ok(name[0], /*first=*/true))
        return false;
    for (size_t i = 1; i < name.size(); ++i)
        if (!ok(name[i], /*first=*/false))
            return false;
    return true;
}

inline bool isValidMetricName(std::string_view name) { return isValidName(name, /*allow_colon=*/true); }
inline bool isValidLabelName(std::string_view name) { return isValidName(name, /*allow_colon=*/false); }

/// OpenMetrics 1.0 metric-family types (https://openmetrics.io/, section MetricType). The Prometheus
/// `untyped` spelling is not part of the vocabulary; it maps to `unknown` (see `normalizeOpenMetricsType`).
inline bool isValidOpenMetricsType(std::string_view type)
{
    return type == "unknown" || type == "gauge" || type == "counter" || type == "stateset"
        || type == "info" || type == "histogram" || type == "gaugehistogram" || type == "summary";
}

/// Normalize a type token to its OpenMetrics 1.0 spelling: the Prometheus `untyped` becomes `unknown`.
/// Every other token is returned verbatim (validity is checked separately with `isValidOpenMetricsType`).
inline String normalizeOpenMetricsType(std::string_view type)
{
    if (type == "untyped")
        return "unknown";
    return String{type};
}

/// `pos` at opening `"`. Decodes `\\`, `\"`, `\n`; rejects other escape sequences. Returns false on malformed input.
inline bool readQuotedLabelValue(std::string_view s, size_t & pos, String & out)
{
    if (pos >= s.size() || s[pos] != '"')
        return false;
    ++pos;
    out.clear();
    while (pos < s.size())
    {
        const char c = s[pos++];
        if (c == '"')
            return true;
        if (c != '\\')
        {
            out.push_back(c);
            continue;
        }
        if (pos >= s.size())
            return false;
        switch (s[pos++])
        {
            case '\\': out.push_back('\\'); break;
            case '"': out.push_back('"'); break;
            case 'n': out.push_back('\n'); break;
            default: return false;
        }
    }
    return false;
}

/// OpenMetrics label values only permit `\\`, `\"`, and `\n` escapes (matching `readQuotedLabelValue`);
/// other control characters cannot round-trip through the reader, so reject them on the writer side.
inline void validateLabelValue(std::string_view s)
{
    for (char c : s)
    {
        if (c == '\\' || c == '"' || c == '\n')
            continue;
        if (static_cast<unsigned char>(c) < 32)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Label value for output format '{}' contains unsupported control character U+{:04X}",
                FORMAT_NAME, static_cast<unsigned char>(c));
    }
}

inline void writeQuotedLabelValue(std::string_view s, WriteBuffer & buf)
{
    validateLabelValue(s);
    writeChar('"', buf);
    for (char c : s)
    {
        switch (c)
        {
            case '\\': writeCString("\\\\", buf); break;
            case '"': writeCString("\\\"", buf); break;
            case '\n': writeCString("\\n", buf); break;
            default: writeChar(c, buf);
        }
    }
    writeChar('"', buf);
}

/// ASCII case-insensitive equality. OpenMetrics `number` special values (`nan`, `inf`, `infinity`)
/// are matched case-insensitively per the spec.
inline bool equalsIgnoreCaseAscii(std::string_view a, std::string_view b)
{
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
    {
        char ca = a[i];
        char cb = b[i];
        if (ca >= 'A' && ca <= 'Z')
            ca = static_cast<char>(ca - 'A' + 'a');
        if (cb >= 'A' && cb <= 'Z')
            cb = static_cast<char>(cb - 'A' + 'a');
        if (ca != cb)
            return false;
    }
    return true;
}

/// `tryReadFloatText` accepts tokens like `.` and `1e+` that OpenMetrics `realnumber` forbids.
inline bool isStrictRealNumberToken(std::string_view token)
{
    if (token.empty())
        return false;

    size_t i = 0;
    if (token[i] == '+' || token[i] == '-')
    {
        ++i;
        if (i >= token.size())
            return false;
    }

    bool has_digit = false;
    if (token[i] >= '0' && token[i] <= '9')
    {
        has_digit = true;
        while (i < token.size() && token[i] >= '0' && token[i] <= '9')
            ++i;
    }

    if (i < token.size() && token[i] == '.')
    {
        ++i;
        while (i < token.size() && token[i] >= '0' && token[i] <= '9')
        {
            has_digit = true;
            ++i;
        }
    }

    if (!has_digit)
        return false;

    if (i < token.size() && (token[i] == 'e' || token[i] == 'E'))
    {
        ++i;
        if (i >= token.size())
            return false;
        if (token[i] == '+' || token[i] == '-')
        {
            ++i;
            if (i >= token.size())
                return false;
        }
        bool exp_digit = false;
        while (i < token.size() && token[i] >= '0' && token[i] <= '9')
        {
            exp_digit = true;
            ++i;
        }
        if (!exp_digit)
            return false;
    }

    return i == token.size();
}

/// Writer side of the timestamp contract. The ClickHouse timestamp is Prometheus-compatible
/// milliseconds; OpenMetrics text expects epoch seconds, so emit `<seconds>.<3-digit-ms>` with
/// trailing-zero stripping (e.g. `1520879607789 -> "1520879607.789"`, `1520879607000 -> "1520879607"`,
/// `-500 -> "-0.5"`, `Int64::min -> "-9223372036854775.808"`).
inline String millisToSecondsString(Int64 ms)
{
    /// `-(Int64::min)` overflows int64, so compute the magnitude in unsigned arithmetic.
    const bool neg = ms < 0;
    const UInt64 abs_ms = neg
        ? (static_cast<UInt64>(-(ms + 1)) + 1u)
        : static_cast<UInt64>(ms);

    const UInt64 seconds = abs_ms / 1000;
    const UInt64 frac = abs_ms % 1000;

    String out;
    if (neg)
        out.push_back('-');
    out += std::to_string(seconds);
    if (frac == 0)
        return out;

    out.push_back('.');
    out.push_back(static_cast<char>('0' + (frac / 100) % 10));
    out.push_back(static_cast<char>('0' + (frac / 10) % 10));
    out.push_back(static_cast<char>('0' + frac % 10));
    /// `frac > 0` guarantees at least one non-zero digit, so the `.` is never the last char.
    while (out.back() == '0')
        out.pop_back();
    return out;
}

/// Exact base-10 decomposition of a finite numeric token of the grammar
/// `[+-]? digits ('.' digits)? ([eE][+-]? digits)?` (at least one mantissa digit). The literal
/// exponent is folded into `point_exponent`, so the value equals
/// `(neg ? -1 : 1) * <digits as integer> * 10^point_exponent`. `digits` is canonical — leading and
/// trailing zeros removed — so numerically-equal tokens share one decomposition: "5", "5.0", "5e0"
/// and "50e-1" all yield neg=false, digits="5", point_exponent=0, and a zero value yields an empty
/// `digits` with point_exponent=0. Returns false for tokens outside the grammar (`inf`, `nan`, an
/// empty mantissa, a `1e+`-style dangling exponent) and for an exponent magnitude beyond 10^9, which
/// no finite value needs and which keeps the `point_exponent` arithmetic clear of overflow.
inline bool decomposeNumber(std::string_view token, bool & neg, String & digits, Int64 & point_exponent)
{
    neg = false;
    digits.clear();
    point_exponent = 0;

    size_t i = 0;
    if (i < token.size() && (token[i] == '+' || token[i] == '-'))
    {
        neg = token[i] == '-';
        ++i;
    }

    String mantissa;
    Int64 frac_len = 0;
    bool has_digit = false;
    while (i < token.size() && token[i] >= '0' && token[i] <= '9')
    {
        mantissa.push_back(token[i++]);
        has_digit = true;
    }
    if (i < token.size() && token[i] == '.')
    {
        ++i;
        while (i < token.size() && token[i] >= '0' && token[i] <= '9')
        {
            mantissa.push_back(token[i++]);
            ++frac_len;
            has_digit = true;
        }
    }
    if (!has_digit)
        return false;

    Int64 exponent = 0;
    if (i < token.size() && (token[i] == 'e' || token[i] == 'E'))
    {
        ++i;
        bool exp_neg = false;
        if (i < token.size() && (token[i] == '+' || token[i] == '-'))
        {
            exp_neg = token[i] == '-';
            ++i;
        }
        bool exp_digit = false;
        while (i < token.size() && token[i] >= '0' && token[i] <= '9')
        {
            if (exponent > 100000000)
                return false;
            exponent = exponent * 10 + (token[i++] - '0');
            exp_digit = true;
        }
        if (!exp_digit)
            return false;
        if (exp_neg)
            exponent = -exponent;
    }

    if (i != token.size())
        return false;

    /// Fold the explicit exponent and the fraction length into a single base-10 point exponent.
    point_exponent = exponent - frac_len;

    size_t start = 0;
    while (start < mantissa.size() && mantissa[start] == '0')
        ++start;
    size_t end = mantissa.size();
    while (end > start && mantissa[end - 1] == '0')
    {
        --end;
        ++point_exponent;
    }
    if (start == end)
    {
        /// All-zero mantissa: canonical zero carries no sign and no exponent.
        neg = false;
        point_exponent = 0;
        return true;
    }
    digits.assign(mantissa, start, end - start);
    return true;
}

/// Reader side of the timestamp contract: convert an OpenMetrics `realnumber` token (epoch seconds,
/// possibly fractional and/or in exponent form) to the millisecond representation stored in the
/// timestamp. The token is decomposed into exact base-10 digits, the seconds->milliseconds
/// scale (10^3) is folded into its exponent, and the result is evaluated with overflow-checked
/// unsigned 64-bit arithmetic so the writer's tokens round-trip back to the same `Int64` across the
/// whole `Int64::min`/`Int64::max` boundary — whether written as `<seconds>.<ms>` or an equivalent
/// exponent form. Sub-millisecond digits are truncated toward zero (the writer never emits >3).
inline Int64 secondsTokenToMillis(std::string_view token, const String & line)
{
    bool neg = false;
    String digits;
    Int64 point_exponent = 0;
    if (!decomposeNumber(token, neg, digits, point_exponent))
        throwIncorrect("Invalid timestamp token", line);

    const Int64 ms_exponent = point_exponent + 3;

    /// Digits left of the millisecond point form the integer ms magnitude; a negative `ms_exponent`
    /// drops the sub-millisecond remainder (truncating toward zero), a non-negative one scales up.
    size_t keep = digits.size();
    if (ms_exponent < 0)
    {
        const UInt64 drop = static_cast<UInt64>(-ms_exponent);
        keep = drop < digits.size() ? digits.size() - static_cast<size_t>(drop) : 0;
    }

    UInt64 magnitude = 0;
    for (size_t k = 0; k < keep; ++k)
        if (common::mulOverflow(magnitude, static_cast<UInt64>(10u), magnitude)
            || common::addOverflow(magnitude, static_cast<UInt64>(digits[k] - '0'), magnitude))
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);
    for (Int64 e = 0; magnitude != 0 && e < ms_exponent; ++e)
        if (common::mulOverflow(magnitude, static_cast<UInt64>(10u), magnitude))
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);

    constexpr UInt64 INT64_MIN_ABS = static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1u;
    constexpr UInt64 INT64_MAX_ABS = static_cast<UInt64>(std::numeric_limits<Int64>::max());

    if (neg)
    {
        if (magnitude > INT64_MIN_ABS)
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);
        if (magnitude == INT64_MIN_ABS)
            return std::numeric_limits<Int64>::min();
        return -static_cast<Int64>(magnitude);
    }
    if (magnitude > INT64_MAX_ABS)
        throwIncorrect("Timestamp value out of Int64 millisecond range", line);
    return static_cast<Int64>(magnitude);
}

}

}
