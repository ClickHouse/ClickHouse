#pragma once

#include <base/types.h>
#include <base/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <cmath>
#include <cstddef>
#include <limits>
#include <map>
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
/// (OpenMetricsTextOutputFormat). Both sides must agree on name/label grammar, the label-value
/// escape set, the millisecond<->seconds timestamp round-trip, and the histogram/summary marker
/// rules, so the single source of truth lives here to keep read and write from drifting.
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

/// Empty `sum`/`count` label values are the internal suffix markers for histogram/summary rows.
inline bool isEmptyMarker(const std::map<String, String> & labels, const String & key)
{
    auto it = labels.find(key);
    return it != labels.end() && it->second.empty();
}

/// Histogram/summary rows must represent exactly one sample kind: bucket (`le`) or quantile
/// (`quantile`), or the `_sum` / `_count` marker - never a combination. Returns true when a row
/// illegally combines more than one. Callers throw their own format-specific error.
inline bool hasMultipleSampleKinds(const String & type, const std::map<String, String> & labels)
{
    if (type != "histogram" && type != "summary")
        return false;

    size_t sample_kinds = 0;
    if (type == "histogram")
    {
        if (labels.contains("le"))
            ++sample_kinds;
    }
    else if (labels.contains("quantile"))
    {
        ++sample_kinds;
    }

    if (isEmptyMarker(labels, "sum"))
        ++sample_kinds;
    if (isEmptyMarker(labels, "count"))
        ++sample_kinds;

    return sample_kinds > 1;
}

/// Histogram series identity: all labels except bucket/meta markers (`le`, empty `count`, empty `sum`).
inline std::map<String, String> histogramSeriesLabels(const std::map<String, String> & labels)
{
    std::map<String, String> series;
    for (const auto & [key, value] : labels)
    {
        if (key == "le")
            continue;
        if (key == "count" && value.empty())
            continue;
        if (key == "sum" && value.empty())
            continue;
        series[key] = value;
    }
    return series;
}

/// Suffix-folding rules shared by the reader (fold `<base>_suffix` into the family) and the writer
/// (emit the suffix from the marker label). `_bucket` belongs only to `histogram` families;
/// `_sum` / `_count` are shared by `histogram` and `summary`.
struct SuffixRule
{
    std::string_view suffix;
    std::string_view synth_label;
    bool histogram_only;
};

inline constexpr std::array<SuffixRule, 3> SUFFIX_RULES = {{
    {"_bucket", "", true},
    {"_sum", "sum", false},
    {"_count", "count", false},
}};

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

/// Writer side of the timestamp contract. The ClickHouse `timestamp` column is Prometheus-compatible
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

/// Reader side of the timestamp contract: convert an OpenMetrics `realnumber` token (epoch seconds,
/// possibly fractional) to the millisecond representation stored in the `timestamp` column. Integer
/// and decimal forms (no exponent) use exact unsigned 64-bit arithmetic so the writer's tokens
/// round-trip back to the same `Int64`, including the `Int64::min`/`Int64::max` boundaries. The
/// exponent form (never emitted by the writer) falls back to `Float64` with a strict range guard.
inline Int64 secondsTokenToMillis(std::string_view token, Float64 ts_value, const String & line)
{
    const bool has_exp = token.find('e') != std::string_view::npos || token.find('E') != std::string_view::npos;

    if (has_exp)
    {
        const Float64 ms_f = ts_value * 1000.0;
        const Float64 upper = std::ldexp(1.0, 63);
        if (!(ms_f > -upper && ms_f < upper))
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);
        return static_cast<Int64>(ms_f);
    }

    /// Strip optional sign once; the caller already accepted the token, so the body below is
    /// `[digits]` or `[digits].[digits]` with at least one digit overall.
    std::string_view body = token;
    bool neg = false;
    if (!body.empty() && (body.front() == '+' || body.front() == '-'))
    {
        neg = body.front() == '-';
        body.remove_prefix(1);
    }

    const size_t dot_pos = body.find('.');
    const std::string_view int_part = (dot_pos == std::string_view::npos) ? body : body.substr(0, dot_pos);
    const std::string_view frac_part = (dot_pos == std::string_view::npos) ? std::string_view{} : body.substr(dot_pos + 1);

    UInt64 abs_seconds = 0;
    for (char c : int_part)
    {
        if (c < '0' || c > '9')
            throwIncorrect("Invalid timestamp token", line);
        UInt64 next = abs_seconds * 10u + static_cast<UInt64>(c - '0');
        if (next < abs_seconds)
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);
        abs_seconds = next;
    }

    /// Pack the first three fractional digits into ms; anything beyond is sub-millisecond and
    /// silently truncated (still validated as digits). The writer never emits >3 frac digits.
    UInt64 abs_frac_ms = 0;
    size_t taken = 0;
    for (char c : frac_part)
    {
        if (c < '0' || c > '9')
            throwIncorrect("Invalid timestamp token", line);
        if (taken < 3)
        {
            abs_frac_ms = abs_frac_ms * 10u + static_cast<UInt64>(c - '0');
            ++taken;
        }
    }
    while (taken < 3)
    {
        abs_frac_ms *= 10u;
        ++taken;
    }

    UInt64 abs_ms_high = 0;
    if (common::mulOverflow(abs_seconds, static_cast<UInt64>(1000u), abs_ms_high))
        throwIncorrect("Timestamp value out of Int64 millisecond range", line);
    UInt64 abs_total = abs_ms_high + abs_frac_ms;
    if (abs_total < abs_ms_high)
        throwIncorrect("Timestamp value out of Int64 millisecond range", line);

    constexpr UInt64 INT64_MIN_ABS = static_cast<UInt64>(std::numeric_limits<Int64>::max()) + 1u;
    constexpr UInt64 INT64_MAX_ABS = static_cast<UInt64>(std::numeric_limits<Int64>::max());

    if (neg)
    {
        if (abs_total > INT64_MIN_ABS)
            throwIncorrect("Timestamp value out of Int64 millisecond range", line);
        if (abs_total == INT64_MIN_ABS)
            return std::numeric_limits<Int64>::min();
        return -static_cast<Int64>(abs_total);
    }
    if (abs_total > INT64_MAX_ABS)
        throwIncorrect("Timestamp value out of Int64 millisecond range", line);
    return static_cast<Int64>(abs_total);
}

}

}
