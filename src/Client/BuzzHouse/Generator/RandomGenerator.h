#pragma once

#include <cstdint>
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

    std::uniform_int_distribution<uint8_t> uints8, digits, hex_digits_dist;

    std::uniform_int_distribution<int16_t> ints16;

    std::uniform_int_distribution<uint16_t> uints16;

    std::uniform_int_distribution<int32_t> ints32, time_hours;

    std::uniform_int_distribution<uint32_t> uints32, dist1, dist2, dist3, dist4, date_years, datetime_years, datetime64_years, months,
        hours, minutes, subseconds, time64_hours, strlens;

    std::uniform_int_distribution<int64_t> ints64;

    std::uniform_int_distribution<uint64_t> uints64;

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

    const DB::Strings jcols{"c0", "c1", "c0.c1", "😆", "😉😉"};

public:
    pcg64_fast generator;

    RandomGenerator(const uint64_t in_seed, const uint32_t min_string_length, const uint32_t max_string_length)
        : seed(in_seed ? in_seed : randomSeed())
        , ints8(std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max())
        , uints8(std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max())
        , digits(static_cast<uint8_t>('0'), static_cast<uint8_t>('9'))
        , hex_digits_dist(0, 15)
        , ints16(std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max())
        , uints16(std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max())
        , ints32(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max())
        , time_hours(-999, 999)
        , uints32(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max())
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
        , subseconds(0, UINT32_C(1000000000))
        , time64_hours(0, 999)
        , strlens(min_string_length, max_string_length)
        , ints64(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max())
        , uints64(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max())
        , zero_one(0, 1)
        , generator(seed)
    {
    }

    const DB::Strings nasty_strings{"a\"a", "b\\tb", "c\\nc", "d\\'d", "e e", "",   "😉", "\"", "\\'",  "\\t",  "\\n",  "--", "0",
                                    "1",    "-1",    "{",     "}",     "(",   ")",  "[",  "]",  ",",    ".",    ";",    ":",  "\\\\",
                                    "/",    "_",     "%",     "*",     "\\0", "{}", "[]", "()", "null", "NULL", "TRUE", "叫", "FALSE"};

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

    uint32_t nextStrlen();

    char nextDigit();

    bool nextBool();

    /// Range [1970-01-01, 2149-06-06]
    String nextDate();

    /// Range [1900-01-01, 2299-12-31]
    String nextDate32();

    /// Range [-999:59:59, 999:59:59]
    String nextTime();

    /// Range [000:00:00, 999:59:59.99999999]
    String nextTime64(bool has_subseconds);

    /// Range [1970-01-01 00:00:00, 2106-02-07 06:28:15]
    String nextDateTime(bool has_subseconds);

    /// Range [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
    String nextDateTime64(bool has_subseconds);

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
        if constexpr (std::is_same_v<T, uint32_t>)
        {
            std::uniform_int_distribution<uint32_t> d{min_val, max_val};
            return d(generator);
        }
        if constexpr (std::is_same_v<T, double>)
        {
            std::uniform_real_distribution<double> d{min_val, max_val};
            return d(generator);
        }
        chassert(0);
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

    String nextString(const String & delimiter, bool allow_nasty, uint32_t limit);

    String nextUUID();

    String nextIPv4();

    String nextIPv6();
};

using RandomSettingParameter = std::function<String(RandomGenerator &)>;

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
