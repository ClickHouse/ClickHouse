#pragma once

#include <cstdint>
#include <functional>
#include <initializer_list>
#include <random>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include <Core/Types.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

namespace BuzzHouse
{

class RandomGenerator
{
private:
    uint64_t seed;

    std::uniform_int_distribution<int8_t> ints8;

    std::uniform_int_distribution<uint8_t> uints8;
    std::uniform_int_distribution<uint8_t> digits;
    std::uniform_int_distribution<uint8_t> hex_digits_dist;

    std::uniform_int_distribution<int16_t> ints16;

    std::uniform_int_distribution<uint16_t> uints16;

    std::uniform_int_distribution<int32_t> ints32;
    std::uniform_int_distribution<int32_t> time_hours;
    std::uniform_int_distribution<int32_t> second_offsets;

    std::uniform_int_distribution<uint32_t> uints32;
    std::uniform_int_distribution<uint32_t> dist1;
    std::uniform_int_distribution<uint32_t> dist2;
    std::uniform_int_distribution<uint32_t> dist3;
    std::uniform_int_distribution<uint32_t> dist4;
    std::uniform_int_distribution<uint32_t> date_years;
    std::uniform_int_distribution<uint32_t> datetime_years;
    std::uniform_int_distribution<uint32_t> datetime64_years;
    std::uniform_int_distribution<uint32_t> months;
    std::uniform_int_distribution<uint32_t> hours;
    std::uniform_int_distribution<uint32_t> minutes;
    std::uniform_int_distribution<uint32_t> subseconds;
    std::uniform_int_distribution<uint32_t> strlens;

    std::uniform_int_distribution<int64_t> ints64;

    std::uniform_int_distribution<uint64_t> uints64;
    std::uniform_int_distribution<uint64_t> full_range;

    std::uniform_real_distribution<double> zero_one;

    std::uniform_int_distribution<uint32_t> days[12]
        = {std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 28),
           std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 30),
           std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 30),
           std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 30),
           std::uniform_int_distribution<uint32_t>(1, 31),
           std::uniform_int_distribution<uint32_t>(1, 30),
           std::uniform_int_distribution<uint32_t>(1, 31)};

    const DB::Strings common_english{
        "is",     "was",   "are",   "be",    "have",  "had",   "were",     "can",   "said",  "use",   "do",      "will",  "would",
        "make",   "like",  "has",   "look",  "write", "go",    "see",      "could", "been",  "call",  "am",      "find",  "did",
        "get",    "come",  "made",  "may",   "take",  "know",  "live",     "give",  "think", "say",   "help",    "tell",  "follow",
        "came",   "want",  "show",  "set",   "put",   "does",  "must",     "ask",   "went",  "read",  "need",    "move",  "try",
        "change", "play",  "spell", "found", "study", "learn", "should",   "add",   "keep",  "start", "thought", "saw",   "turn",
        "might",  "close", "seem",  "open",  "begin", "got",   "run",      "walk",  "began", "grow",  "took",    "carry", "hear",
        "stop",   "miss",  "eat",   "watch", "let",   "cut",   "talk",     "being", "leave", "water", "day",     "part",  "sound",
        "work",   "place", "year",  "back",  "thing", "name",  "sentence", "man",   "line",  "boy"};

    const DB::Strings common_chinese{
        "认识你很高兴", "美国", "叫", "名字", "你们", "日本", "哪国人", "爸爸", "兄弟姐妹", "漂亮", "照片", "😉"};

    /// Use bad_utf8 on x' strings!
    const DB::Strings bad_utf8{
        "00", /// Null byte
        "FF",
        "C328",
        "A0A1",
        "E228A1",
        "E28228",
        "F0288CBC",
        "F09028BC",
        "F0288C28",
        "C328A0A1",
        "E28228FF",
        "F0288CBCF0288CBC",
        "C328FF",
        "AAC328"};

    const DB::Strings jcols{
        "_",
        ".",
        "1",
        "叫",
        "c0",
        "c1",
        "c0.c1",
        "😆",
        "😉😉",
        "123",
        "0",
        "a.b.c",
        "a.b.c.d",
        "select",
        "from",
        "where",
        "key with space",
        "key-with-dash"};

public:
    pcg64_fast generator;

    RandomGenerator(const uint64_t in_seed, const uint32_t min_string_length, const uint32_t max_string_length, const bool limited)
        : seed(in_seed ? in_seed : randomSeed())
        , ints8(limited ? -50 : std::numeric_limits<int8_t>::min(), limited ? 50 : std::numeric_limits<int8_t>::max())
        , uints8(limited ? 0 : std::numeric_limits<uint8_t>::min(), limited ? 100 : std::numeric_limits<uint8_t>::max())
        , digits(static_cast<uint8_t>('0'), static_cast<uint8_t>('9'))
        , hex_digits_dist(0, 15)
        , ints16(limited ? -50 : std::numeric_limits<int16_t>::min(), limited ? 50 : std::numeric_limits<int16_t>::max())
        , uints16(limited ? 0 : std::numeric_limits<uint16_t>::min(), limited ? 100 : std::numeric_limits<uint16_t>::max())
        , ints32(limited ? -50 : std::numeric_limits<int32_t>::min(), limited ? 50 : std::numeric_limits<int32_t>::max())
        , time_hours(-999, 999)
        , second_offsets(-10, 80)
        , uints32(limited ? 0 : std::numeric_limits<uint32_t>::min(), limited ? 100 : std::numeric_limits<uint32_t>::max())
        , dist1(UINT32_C(1), UINT32_C(10))
        , dist2(UINT32_C(1), UINT32_C(100))
        , dist3(UINT32_C(1), UINT32_C(1000))
        , dist4(UINT32_C(1), UINT32_C(2))
        , date_years(0, 2149 - 1970)
        , datetime_years(0, 2106 - 1970)
        , datetime64_years(0, 2299 - 1900)
        , months(1, 12)
        , hours(0, 23)
        , minutes(0, 59)
        , subseconds(0, UINT32_C(999999999))
        , strlens(min_string_length, max_string_length)
        , ints64(limited ? -50 : std::numeric_limits<int64_t>::min(), limited ? 50 : std::numeric_limits<int64_t>::max())
        , uints64(limited ? 0 : std::numeric_limits<uint64_t>::min(), limited ? 100 : std::numeric_limits<uint64_t>::max())
        , full_range(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max())
        , zero_one(0, 1)
        , generator(seed)
    {
    }

    const DB::Strings nasty_strings{
        "a\"a",
        "b\\tb",
        "c\\nc",
        "e e",
        "",
        "😉",
        "\"",
        "\\t",
        "\\n",
        "--",
        "0",
        "1",
        "-1",
        "{",
        "}",
        "(",
        ")",
        "[",
        "]",
        ",",
        ".",
        ";",
        ":",
        "\\\\",
        "/",
        "_",
        "%",
        "*",
        "\\0",
        "{}",
        "[]",
        "()",
        "null",
        "NULL",
        "TRUE",
        "叫",
        "FALSE",
        /// Format string probes
        "%s",
        "%d",
        "%n",
        "{0}",
        "{{}}",
        /// Numeric-looking strings (test implicit casts)
        "NaN",
        "Inf",
        "-Inf",
        "1e5",
        "1.0",
        "0.0",
        /// Date/time-looking strings (test type coercions)
        "2024-01-01",
        "00:00:00",
        "1970-01-01 00:00:00",
        /// Windows line ending, other special whitespace
        "\\r\\n",
        "\\r",
        /// Extra punctuation useful for parser probing
        "?",
        "@",
        "#",
        "$"};

    /// Parts for building nasty SQL identifiers (backtick-quoted).
    /// Backtick inside a backtick-quoted identifier must be doubled (`` ` `` → ` `` `).
    /// Counter appended for uniqueness.
    const DB::Strings nasty_identifiers{
        "0", /// 0 digit
        "1", /// 1 digit
        " ", /// simple space
        " - ", /// space-hyphen-space
        "#", /// hash
        "@", /// at-sign
        "$", /// dollar sign
        "\"", /// double quote
        "\\", /// backslash
        "`", /// backtick (will be doubled when backtick-quoted)
        /// Emoji and multi-codepoint sequences
        "😉", /// emoji (U+1F609, 4-byte UTF-8)
        "🔥💧", /// two emoji
        "👨‍👩‍👧", /// ZWJ family sequence (multiple codepoints joined by U+200D)
        "🇺🇸", /// flag emoji (regional indicator pair)
        /// CJK and other scripts
        "叫", /// Chinese character
        "认识你很高兴", /// Chinese phrase
        "日本語", /// Japanese
        "한국어", /// Korean
        "العربية", /// Arabic (RTL)
        "עברית", /// Hebrew (RTL)
        "हिन्दी", /// Hindi (Devanagari with combining marks)
        /// Latin variants and diacritics
        "é", /// accented Latin (precomposed)
        "e\xCC\x81", /// accented Latin (decomposed: e + combining acute, U+0301)
        "ñ", /// tilde
        "ü", /// umlaut
        "ß", /// German sharp-s
        "ŀ", /// Latin with middle dot
        /// Greek, Cyrillic
        "α", /// Greek letter
        "Ω", /// Greek capital omega
        "Привет", /// Cyrillic
        /// Special Unicode spaces and invisible characters
        "\xE2\x80\x8B", /// zero-width space (U+200B)
        "\xE2\x80\x8C", /// zero-width non-joiner (U+200C)
        "\xE2\x80\x8F", /// right-to-left mark (U+200F)
        "\xEF\xBB\xBF", /// BOM / zero-width no-break space (U+FEFF)
        "\xC2\xA0", /// non-breaking space (U+00A0)
        "\xE2\x80\x83", /// em space (U+2003)
        /// Lookalike / homoglyph characters
        "\xCF\x83", /// Greek small sigma σ (looks like o)
        "\xD0\xBE", /// Cyrillic small o (looks like Latin o)
        "\xE2\x84\x93", /// script small l ℓ (U+2113)
        /// Invalid UTF-8 sequences
        "\xFF", /// invalid UTF-8: lone 0xFF byte
        "\xC3\x28", /// invalid UTF-8: bad 2-byte sequence
        "\xA0\xA1", /// invalid UTF-8: continuation bytes without lead
        "\xE2\x28\xA1", /// invalid UTF-8: bad 3-byte sequence
        "\xF0\x28\x8C\xBC", /// invalid UTF-8: bad 4-byte sequence
    };
    const DB::Strings nasty_identifier_keywords{
        "SELECT",
        "FROM",
        "WHERE",
        "ORDER",
        "GROUP",
        "INDEX",
        "TABLE",
        "CREATE",
        "DROP",
        "PROJECTION",
    };

    uint64_t getSeed() const;

    uint32_t nextSmallNumber();

    uint32_t nextMediumNumber();

    uint32_t nextLargeNumber();

    uint8_t nextRandomUInt8();

    int8_t nextRandomInt8();

    uint16_t nextRandomUInt16();

    int16_t nextRandomInt16();

    uint32_t nextRandomUInt32();

    int32_t nextRandomInt32();

    uint64_t nextRandomUInt64();

    int64_t nextRandomInt64();

    uint64_t nextInFullRange();

    uint32_t nextStrlen();

    char nextDigit();

    bool nextBool();

    /// Range [1970-01-01, 2149-06-06]
    String nextDate(const String & separator, bool allow_func);

    /// Range [1900-01-01, 2299-12-31]
    String nextDate32(const String & separator, bool allow_func);

    /// Range [-999:59:59, 999:59:59]
    String nextTime(const String & separator, bool allow_func);

    /// Range [-999:59:59.999999999, 999:59:59.999999999]
    String nextTime64(const String & separator, bool allow_func, bool has_subseconds);

    /// Range [1970-01-01 00:00:00, 2106-02-07 06:28:15]
    String nextDateTime(const String & separator, bool allow_func, bool has_subseconds);

    /// Range [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
    String nextDateTime64(const String & separator, bool allow_func, bool has_subseconds);

    template <typename T>
    T thresholdGenerator(const double always_on_prob, const double always_off_prob, T min_val, T max_val)
    {
        const double tmp = zero_one(generator);

        if (tmp <= always_on_prob)
        {
            return min_val;
        }
        if (tmp <= always_on_prob + always_off_prob)
        {
            return max_val;
        }
        if (tmp <= always_on_prob + always_off_prob + 0.001)
        {
            if constexpr (std::is_unsigned_v<T>)
            {
                return (tmp <= always_on_prob + always_off_prob + 0.0003) ? 0 : std::numeric_limits<T>::max();
            }
            if constexpr (std::is_signed_v<T>)
            {
                return (tmp <= always_on_prob + always_off_prob + 0.0005) ? std::numeric_limits<T>::min() : std::numeric_limits<T>::max();
            }
            if constexpr (std::is_floating_point_v<T>)
            {
                if (tmp <= always_on_prob + always_off_prob + 0.0003)
                {
                    return std::numeric_limits<T>::min();
                }
                if (max_val >= 0.9 && max_val <= 1.1)
                {
                    return max_val;
                }
                return std::numeric_limits<T>::max();
            }
            UNREACHABLE();
        }
        if constexpr (std::is_integral_v<T>)
        {
            std::uniform_int_distribution<T> d{min_val, max_val};
            return d(generator);
        }
        if constexpr (std::is_floating_point_v<T>)
        {
            std::uniform_real_distribution<T> d{min_val, max_val};
            return d(generator);
        }
        UNREACHABLE();
        return 0;
    }

    double randomGauss(double mean, double stddev);

    double randomZeroOne();

    template <typename T>
    T randomInt(const T min, const T max)
    {
        std::uniform_int_distribution<T> d(min, max);
        return d(generator);
    }

    template <typename Container>
    const auto & pickRandomly(const Container & container)
    {
        std::uniform_int_distribution<size_t> d{0, container.size() - 1};
        auto it = container.begin();
        std::advance(it, d(generator));

        if constexpr (requires { it->first; })
            return it->first;
        else
            return *it;
    }

    template <typename K, typename V>
    const V & pickValueRandomlyFromMap(const std::unordered_map<K, V> & vals)
    {
        std::uniform_int_distribution<size_t> d{0, vals.size() - 1};
        auto it = vals.begin();
        std::advance(it, d(generator));
        return it->second;
    }

    String nextJSONCol();

    String nextTokenString();

    String nextIdentifier(const String & prefix, uint32_t counter, bool allow_nasty);

    String nextString(const String & delimiter, bool allow_nasty, uint32_t limit);

    String nextUUID();

    String nextIPv4();

    String nextIPv6();

    /// Weighted random dispatch: picks one option proportional to its weight and calls its action.
    /// Options with weight 0 are skipped. At least one weight must be non-zero.
    void pickWeighted(std::initializer_list<std::pair<uint32_t, std::function<void()>>> options);
};

class FuzzConfig;

using RandomSettingParameter = std::function<String(RandomGenerator &, FuzzConfig &)>;

struct CHSetting
{
public:
    const RandomSettingParameter random_func;
    const std::unordered_set<String> oracle_values;
    const bool changes_behavior;

    CHSetting(const RandomSettingParameter & rf, const std::unordered_set<String> & ov, const bool cb)
        : random_func(rf)
        , oracle_values(ov)
        , changes_behavior(cb)
    {
    }
};

}
