#include <Interpreters/TokenMasks.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <bit>
#include <bitset>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Cheap byte prechecks of the built-in patterns, so that most candidate positions run no regular expression.
/// Each one is a necessary condition for a match at `data`; the first byte is already checked by the first-byte table.

bool precheckIsoTimestamp(const char * data, size_t length)
{
    return length >= 10 && isNumericASCII(data[0]) && isNumericASCII(data[1]) && isNumericASCII(data[2]) && isNumericASCII(data[3])
        && data[4] == '-';
}

bool precheckAccessLogTimestamp(const char * data, size_t length)
{
    /// 04/Nov/2026:10:15:30 +0000
    return length >= 26 && data[2] == '/' && data[6] == '/';
}

bool precheckSyslogTimestamp(const char * data, size_t length)
{
    /// Nov 15 10:15:30
    return length >= 15 && data[3] == ' ';
}

bool precheckIPv4(const char * data, size_t length)
{
    size_t i = 0;
    while (i < length && i < 3 && isNumericASCII(data[i]))
        ++i;
    return i > 0 && i < length && data[i] == '.';
}

bool precheckIPv6(const char * data, size_t length)
{
    size_t i = 0;
    while (i < length && i < 4 && isHexDigit(data[i]))
        ++i;
    return i > 0 && i < length && data[i] == ':';
}

bool precheckUUID(const char * data, size_t length)
{
    return length >= 36 && data[8] == '-';
}

bool precheckHexWithPrefix(const char * data, size_t length)
{
    return length >= 3 && (data[1] == 'x' || data[1] == 'X') && isHexDigit(data[2]);
}

constexpr ByteSetLookup path_segment_chars = ByteSetLookup::fromPredicate([](char c)
{
    return isAlphaNumericASCII(c) || c == '.' || c == '_' || c == '-' || c == '~' || c == '%' || c == '+' || c == '@';
});

constexpr ByteSetLookup url_scheme_chars = ByteSetLookup::fromPredicate([](char c)
{
    return isAlphaNumericASCII(c) || c == '+' || c == '.' || c == '-';
});

/// The end of the run of bytes in `set` from `pos`.
size_t skipBytes(const ByteSetLookup & set, const char * data, size_t length, size_t pos)
{
    return set.find<false>(data + pos, data + length) - data;
}

bool precheckWindowsPath(const char * data, size_t length)
{
    return length >= 4 && data[1] == ':' && data[2] == '\\';
}

/// Hand-written matchers of the built-in patterns that can scan far without matching, or are tried at many positions.
/// They return exactly the length of the longest match of the pattern, or 0. On failure, they may set `no_match_until` to a position before which every
/// later start fails too, so that the scan of an input stays linear.

/// `[a-z0-9._\-~%+@]+(?:[/\\][a-z0-9._\-~%+@]+){2,}`. The first segment ends at the first byte that is not a segment
/// character, from any start within it, so all these starts give the same result.
size_t matchPath(const re2::RE2 * /*re*/, const char * data, size_t length, const char *& no_match_until)
{
    const size_t first_segment_end = skipBytes(path_segment_chars, data, length, 0);

    if (first_segment_end == 0)
        return 0;

    size_t pos = first_segment_end;
    size_t slashes = 0;
    size_t match_end = 0;

    while (pos < length && (data[pos] == '/' || data[pos] == '\\'))
    {
        const size_t segment_end = skipBytes(path_segment_chars, data, length, pos + 1);

        if (segment_end == pos + 1)
            break;

        pos = segment_end;
        if (++slashes >= 2)
            match_end = pos;
    }

    if (match_end == 0)
        no_match_until = data + first_segment_end;

    return match_end;
}

bool isRE2Space(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r';
}

/// `[a-z][a-z0-9+.\-]*://[^\s]+`. The scheme ends at the first byte that is not a scheme character, from any start within
/// it, so all these starts fail if the first one fails after the scheme.
size_t matchURL(const re2::RE2 * /*re*/, const char * data, size_t length, const char *& no_match_until)
{
    if (!isAlphaASCII(data[0]))
        return 0;

    const size_t scheme_end = skipBytes(url_scheme_chars, data, length, 1);

    if (scheme_end + 3 >= length || data[scheme_end] != ':' || data[scheme_end + 1] != '/' || data[scheme_end + 2] != '/'
        || isRE2Space(data[scheme_end + 3]))
    {
        no_match_until = data + scheme_end;
        return 0;
    }

    size_t end = scheme_end + 4;
    while (end < length && !isRE2Space(data[end]))
        ++end;

    return end;
}

/// `\d+(?:\.\d+)?(?:e[-+]?\d+)?(?:[a-z]+|%)?`. Taking each optional part as far as it goes never shortens the match:
/// the later parts cannot consume the digits of an earlier one, and a valid exponent is longer than the letter `e` alone.
size_t matchNumber(const re2::RE2 * /*re*/, const char * data, size_t length, const char *& /*no_match_until*/)
{
    auto skip_digits = [&](size_t pos)
    {
        while (pos < length && isNumericASCII(data[pos]))
            ++pos;
        return pos;
    };

    size_t pos = skip_digits(0);
    if (pos == 0)
        return 0;

    if (pos + 1 < length && data[pos] == '.' && isNumericASCII(data[pos + 1]))
        pos = skip_digits(pos + 1);

    if (pos < length && (data[pos] == 'e' || data[pos] == 'E'))
    {
        size_t exponent = pos + 1;
        if (exponent < length && (data[exponent] == '+' || data[exponent] == '-'))
            ++exponent;
        if (exponent < length && isNumericASCII(data[exponent]))
            pos = skip_digits(exponent);
    }

    if (pos < length && data[pos] == '%')
        return pos + 1;

    while (pos < length && isAlphaASCII(data[pos]))
        ++pos;

    return pos;
}

constexpr ByteSetLookup hex_digit_chars = ByteSetLookup::fromPredicate([](char c)
{
    return isNumericASCII(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
});

constexpr ByteSetLookup decimal_digit_chars = ByteSetLookup::fromPredicate([](char c) { return isNumericASCII(c); });

/// `[0-9a-f]{8,}`, accepted only if it contains a decimal digit. The match is the whole run of hexadecimal digits. A later
/// start within a rejected run sees a shorter run without digits, so it is rejected too. Without this, separators of
/// `splitByString` that are hexadecimal digits would make each base token rescan the rest of the run.
size_t matchHex(const re2::RE2 * /*re*/, const char * data, size_t length, const char *& no_match_until)
{
    const size_t run_end = skipBytes(hex_digit_chars, data, length, 0);
    if (run_end >= 8 && decimal_digit_chars.find<true>(data, data + run_end) != data + run_end)
        return run_end;

    no_match_until = data + run_end;
    return 0;
}

bool containsDigit(std::string_view text)
{
    return std::ranges::any_of(text, [](char c) { return isNumericASCII(c); });
}

/// Returns the length of the accepted longest match of a built-in pattern at `data`, or 0.
/// `re` is the compiled pattern. On failure, may set `no_match_until`, see `TokenMasks::ScanState`.
using MatchFunction = size_t (*)(const re2::RE2 * re, const char * data, size_t length, const char *& no_match_until);

/// Matches with RE2 after a necessary condition `precheck`. With `needs_digit`, a match is accepted only if it contains
/// a decimal digit (so that `deadbeef` is not `hex` and `DB::Add` is not `ip`).
template <bool (*precheck)(const char * data, size_t length), bool needs_digit>
size_t matchRegexp(const re2::RE2 * re, const char * data, size_t length, const char *& /*no_match_until*/)
{
    if (!precheck(data, length))
        return 0;

    std::string_view match;
    if (!re->Match(std::string_view(data, length), 0, length, re2::RE2::ANCHOR_START, &match, 1))
        return 0;

    if (needs_digit && !containsDigit(match))
        return 0;

    return match.size();
}

struct BuiltinPatternDefinition
{
    std::string_view mask;
    /// The definition of the pattern: `match` returns the length of its longest match.
    std::string_view pattern;
    MatchFunction match;
    /// For tests: whether `match` accepts only matches with a decimal digit.
    bool needs_digit;
};

/// The built-in masks. Tokens in index parts depend on these patterns: a changed pattern needs a new mask name.
/// No pattern can begin with a separator of `splitByNonAlpha`, because masks are tried only at base-token starts.
constexpr BuiltinPatternDefinition builtin_pattern_definitions[] = {
    /// ISO 8601.
    {"timestamp", R"(\d{4}-\d{2}-\d{2}(?:[t ]\d{2}:\d{2}(?::\d{2})?(?:[.,]\d+)?(?:z|[+-]\d{2}(?::?\d{2})?)?)?)", matchRegexp<precheckIsoTimestamp, false>, false},
    /// Access log, without the brackets.
    {"timestamp", R"(\d{2}/[a-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})", matchRegexp<precheckAccessLogTimestamp, false>, false},
    /// Syslog.
    {"timestamp", R"((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec) [ \d]\d \d{2}:\d{2}:\d{2})", matchRegexp<precheckSyslogTimestamp, false>, false},
    /// IPv4, without the port.
    {"ip", R"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", matchRegexp<precheckIPv4, false>, false},
    /// IPv6, the forms beginning with a hexadecimal group.
    {"ip", R"((?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,6}:(?:[0-9a-f]{1,4}:){0,5}[0-9a-f]{1,4})", matchRegexp<precheckIPv6, true>, true},
    {"uuid", R"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", matchRegexp<precheckUUID, false>, false},
    {"hex", R"(0x[0-9a-f]+)", matchRegexp<precheckHexWithPrefix, false>, false},
    {"hex", R"([0-9a-f]{8,})", matchHex, true},
    /// No sign: a sign is a separator.
    {"number", R"(\d+(?:\.\d+)?(?:e[-+]?\d+)?(?:[a-z]+|%)?)", matchNumber, false},
    /// Segments joined by two or more slashes, from the first segment.
    {"path", R"([a-z0-9._\-~%+@]+(?:[/\\][a-z0-9._\-~%+@]+){2,})", matchPath, false},
    /// Windows drive.
    {"path", R"([a-z]:\\[a-z0-9._\-~%+@/\\]+)", matchRegexp<precheckWindowsPath, false>, false},
    {"url", R"([a-z][a-z0-9+.\-]*://[^\s]+)", matchURL, false},
};

static_assert(std::size(builtin_pattern_definitions) <= TokenMasks::MAX_BUILTIN_PATTERNS);

/// RE2 needs memory for the compiled program and the DFA cache. The limit is fixed, so that a definition of custom masks
/// is accepted or rejected the same way on every server.
constexpr int64_t CUSTOM_MASKS_MAX_MEM = 8 << 20;

re2::RE2::Options getOptions(bool latin1)
{
    re2::RE2::Options options;
    options.set_encoding(latin1 ? re2::RE2::Options::EncodingLatin1 : re2::RE2::Options::EncodingUTF8);
    options.set_case_sensitive(false);
    options.set_longest_match(true);
    options.set_never_capture(true);
    options.set_log_errors(false);
    options.set_max_mem(CUSTOM_MASKS_MAX_MEM);
    return options;
}

/// What a node of a parse tree can start with, and whether it can match the empty string.
struct FirstBytesInfo
{
    std::bitset<256> bytes;
    bool nullable = false;
};

/// Computes `FirstBytesInfo` of a parse tree bottom-up. The result is a superset, because it is only a filter.
/// Also detects anchors and word-boundary assertions.
class FirstBytesWalker : public re2::Regexp::Walker<FirstBytesInfo>
{
public:
    /// `re2::Regexp::Walker` leaves its `max_visits_` uninitialized until `Walk` sets it.
    explicit FirstBytesWalker(bool latin1_) : latin1(latin1_) {} // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)

    bool has_assertion = false;

    FirstBytesInfo PostVisit(re2::Regexp * re, FirstBytesInfo /*parent_arg*/, FirstBytesInfo /*pre_arg*/, FirstBytesInfo * child_args, int nchild_args) override
    {
        FirstBytesInfo result;

        switch (re->op())
        {
            case re2::kRegexpNoMatch:
                break;
            case re2::kRegexpEmptyMatch:
                result.nullable = true;
                break;
            case re2::kRegexpLiteral:
                addRune(result.bytes, re->rune(), (re->parse_flags() & re2::Regexp::FoldCase) != 0);
                break;
            case re2::kRegexpLiteralString:
                if (re->nrunes() > 0)
                    addRune(result.bytes, re->runes()[0], (re->parse_flags() & re2::Regexp::FoldCase) != 0);
                else
                    result.nullable = true;
                break;
            case re2::kRegexpConcat:
                /// The first element, and the next ones while all previous ones are nullable.
                result.nullable = true;
                for (int i = 0; i < nchild_args; ++i)
                {
                    result.bytes |= child_args[i].bytes;
                    if (!child_args[i].nullable)
                    {
                        result.nullable = false;
                        break;
                    }
                }
                break;
            case re2::kRegexpAlternate:
                for (int i = 0; i < nchild_args; ++i)
                {
                    result.bytes |= child_args[i].bytes;
                    result.nullable |= child_args[i].nullable;
                }
                break;
            case re2::kRegexpStar:
            case re2::kRegexpQuest:
                result.bytes = child_args[0].bytes;
                result.nullable = true;
                break;
            case re2::kRegexpPlus:
            case re2::kRegexpCapture:
                result = child_args[0];
                break;
            case re2::kRegexpRepeat:
                /// `max` is -1 for no upper bound.
                if (re->max() != 0)
                    result.bytes = child_args[0].bytes;
                result.nullable = re->min() == 0 || child_args[0].nullable;
                break;
            case re2::kRegexpAnyChar:
            case re2::kRegexpAnyByte:
                result.bytes.set();
                break;
            case re2::kRegexpCharClass:
                if (auto * char_class = re->cc())
                {
                    /// The first byte of the encoding grows with the code point, so a range of code points starts with
                    /// bytes from the first byte of its lowest to the first byte of its highest code point.
                    for (const auto & range : *char_class)
                    {
                        for (size_t byte = firstByte(range.lo); byte <= firstByte(range.hi); ++byte)
                            result.bytes.set(byte);
                    }
                }
                else
                {
                    result.bytes.set();
                }
                break;
            case re2::kRegexpBeginLine:
            case re2::kRegexpEndLine:
            case re2::kRegexpWordBoundary:
            case re2::kRegexpNoWordBoundary:
            case re2::kRegexpBeginText:
            case re2::kRegexpEndText:
                has_assertion = true;
                result.nullable = true;
                break;
            case re2::kRegexpHaveMatch:
                result.nullable = true;
                break;
        }

        return result;
    }

    FirstBytesInfo ShortVisit(re2::Regexp * /*re*/, FirstBytesInfo /*parent_arg*/) override
    {
        FirstBytesInfo result;
        result.bytes.set();
        result.nullable = true;
        return result;
    }

private:
    size_t firstByte(int rune) const
    {
        const auto code_point = static_cast<size_t>(rune);
        if (latin1 || code_point < 0x80)
            return code_point;
        if (code_point < 0x800)
            return 0xC0 | (code_point >> 6);
        if (code_point < 0x10000)
            return 0xE0 | (code_point >> 12);
        return 0xF0 | (code_point >> 18);
    }

    void addRune(std::bitset<256> & bytes, int rune, bool fold_case) const
    {
        bytes.set(firstByte(rune));

        if (!fold_case)
            return;

        if (rune < 0x80)
        {
            const char c = static_cast<char>(rune);
            bytes.set(static_cast<UInt8>(toLowerASCII(c)));
            bytes.set(static_cast<UInt8>(toUpperASCII(c)));
        }
        else
        {
            /// Case folding of a non-ASCII letter may change the length of its encoding.
            for (size_t byte = 0x80; byte < 256; ++byte)
                bytes.set(byte);
        }
    }

    bool latin1;
};

ByteSetLookup toByteSetLookup(const std::bitset<256> & bytes)
{
    ByteSetLookup result;
    for (size_t byte = 0; byte < 256; ++byte)
    {
        if (bytes.test(byte))
            result.add(static_cast<char>(byte));
    }
    return result;
}

}

struct TokenMasks::BuiltinPattern
{
    const BuiltinPatternDefinition & definition;
    MatchFunction match;
    std::unique_ptr<re2::RE2> re;
    ByteSetLookup first_bytes;
};

namespace
{

const std::vector<TokenMasks::BuiltinPattern> & getBuiltinPatterns()
{
    static const std::vector<TokenMasks::BuiltinPattern> patterns = []
    {
        std::vector<TokenMasks::BuiltinPattern> result;
        for (const auto & definition : builtin_pattern_definitions)
        {
            auto re = std::make_unique<re2::RE2>(definition.pattern, getOptions(/*latin1=*/ true));
            if (!re->ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compile built-in mask '{}': {}", definition.mask, re->error());

            auto first_bytes = TokenMasks::getFirstBytes(*re);
            result.push_back({definition, definition.match, std::move(re), first_bytes});
        }
        return result;
    }();

    return patterns;
}

}

const std::vector<String> & TokenMasks::getBuiltinMaskNames()
{
    static const std::vector<String> names = []
    {
        std::vector<String> result;
        for (const auto & definition : builtin_pattern_definitions)
        {
            if (std::ranges::find(result, definition.mask) == result.end())
                result.emplace_back(definition.mask);
        }
        return result;
    }();

    return names;
}

ByteSetLookup TokenMasks::getFirstBytes(const re2::RE2 & pattern)
{
    FirstBytesWalker walker(pattern.options().encoding() == re2::RE2::Options::EncodingLatin1);
    auto info = walker.Walk(pattern.Regexp(), FirstBytesInfo{});

    if (walker.has_assertion)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            R"(Mask '{}' contains an anchor or a word-boundary assertion (`^`, `$`, `\A`, `\z`, `\b`, `\B`), which is not supported)",
            pattern.pattern());

    return toByteSetLookup(info.bytes);
}

TokenMasks::TokenMasks(const std::vector<String> & builtin_names_, const std::vector<String> & custom_patterns_)
{
    const auto & all_names = getBuiltinMaskNames();
    for (const auto & name : builtin_names_)
    {
        if (std::ranges::find(all_names, name) == all_names.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Unknown built-in mask '{}'. Built-in masks: {}", name, fmt::join(all_names, ", "));
    }

    for (const auto & name : all_names)
    {
        if (std::ranges::find(builtin_names_, name) != builtin_names_.end())
            builtin_names.push_back(name);
    }

    for (const auto & pattern : getBuiltinPatterns())
    {
        if (std::ranges::find(builtin_names, pattern.definition.mask) == builtin_names.end())
            continue;

        for (size_t byte = 0; byte < 256; ++byte)
        {
            if (pattern.first_bytes.contains(static_cast<char>(byte)))
            {
                builtin_candidates[byte] |= static_cast<UInt16>(1u << builtin_patterns.size());
                first_bytes.add(static_cast<char>(byte));
            }
        }
        builtin_patterns.push_back(&pattern);
    }

    custom_patterns = custom_patterns_;
    std::ranges::sort(custom_patterns);
    custom_patterns.erase(std::unique(custom_patterns.begin(), custom_patterns.end()), custom_patterns.end());

    if (custom_patterns.empty())
        return;

    if (custom_patterns.size() > MAX_CUSTOM_MASKS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many custom masks: {}, the maximum is {}", custom_patterns.size(), MAX_CUSTOM_MASKS);

    const auto options = getOptions(/*latin1=*/ false);
    String alternation;

    for (const auto & pattern : custom_patterns)
    {
        if (pattern.size() > MAX_CUSTOM_MASK_SIZE)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Custom mask is too long: {} bytes, the maximum is {}", pattern.size(), MAX_CUSTOM_MASK_SIZE);

        /// Each pattern is compiled alone first, for the error message.
        re2::RE2 re(pattern, options);
        if (!re.ok())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot compile custom mask '{}': {}", pattern, re.error());

        getFirstBytes(re);

        if (re2::RE2::FullMatch("", re))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom mask '{}' matches the empty string, which is not supported", pattern);

        if (!alternation.empty())
            alternation += '|';
        alternation += "(?:";
        alternation += pattern;
        alternation += ')';
    }

    custom = std::make_unique<re2::RE2>(alternation, options);
    if (!custom->ok())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot compile custom masks: {}", custom->error());

    custom_first_bytes = getFirstBytes(*custom);
    for (size_t byte = 0; byte < 256; ++byte)
    {
        if (custom_first_bytes.contains(static_cast<char>(byte)))
            first_bytes.add(static_cast<char>(byte));
    }
}

TokenMasks::~TokenMasks() = default;

std::vector<TokenMasks::BuiltinPatternSpec> TokenMasks::getBuiltinPatternSpecs()
{
    std::vector<BuiltinPatternSpec> result;
    for (const auto & definition : builtin_pattern_definitions)
        result.push_back({definition.mask, definition.pattern, definition.needs_digit});
    return result;
}

size_t TokenMasks::matchAt(const char * data, size_t length, ScanState & state) const
{
    const char first_byte = data[0];
    const BuiltinPattern * const * patterns = builtin_patterns.data();
    size_t longest = 0;

    /// Built-in patterns are matched one by one, because a failed post-check must not hide the match of another pattern.
    for (UInt32 candidates = builtin_candidates[static_cast<UInt8>(first_byte)]; candidates != 0; candidates &= candidates - 1)
    {
        const size_t i = std::countr_zero(candidates);
        const char *& no_match_until = state.no_match_until[i];
        if (no_match_until && data < no_match_until)
            continue;

        const BuiltinPattern & pattern = *patterns[i];
        longest = std::max(longest, pattern.match(pattern.re.get(), data, length, no_match_until));
    }

    std::string_view match;
    if (custom && custom_first_bytes.contains(first_byte)
        && custom->Match(std::string_view(data, length), 0, length, re2::RE2::ANCHOR_START, &match, 1))
        longest = std::max(longest, match.size());

    return longest;
}

}
