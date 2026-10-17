#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <base/find_symbols.h>
#include <gtest/gtest.h>


template <char ... symbols>
void test_find_first_not(const std::string & haystack, std::size_t expected_pos)
{
    const char * begin = haystack.data();
    const char * end = haystack.data() + haystack.size();

    ASSERT_EQ(begin + expected_pos, find_first_not_symbols<symbols...>(begin, end));
}


template <char... symbols>
static void test_compile_time_boundaries()
{
    const std::array<char, sizeof...(symbols)> needles {symbols...};
    char non_needle = 'a';
    while (std::find(needles.begin(), needles.end(), non_needle) != needles.end())
        ++non_needle;
    const bool contains_null = std::find(needles.begin(), needles.end(), '\0') != needles.end();
    const std::array<size_t, 45> sizes {
        0, 1,
        15, 16, 17,
        31, 32, 33,
        47, 48, 49,
        63, 64, 65,
        127, 128, 129,
        255, 256,
        511, 512, 513,
        767, 768, 769,
        1023, 1024, 1025,
        1039, 1040, 1041,
        1055, 1056, 1057,
        1087, 1088, 1089,
        1119, 1120, 1121,
        1151, 1152, 1153,
        1536, 1537,
    };
    const std::array<size_t, 31> positions {
        0, 1, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 65, 127, 128, 255, 256,
        511, 512, 543, 544, 575, 576, 607, 608, 639, 640, 767, 1023, 1119, 1120,
    };

    for (const size_t size : sizes)
    {
        std::string haystack(size, non_needle);
        const char * begin = haystack.data();
        const char * end = begin + haystack.size();

        ASSERT_EQ(find_first_symbols<symbols...>(begin, end), end) << "size: " << size;
        ASSERT_EQ(find_first_symbols_or_null<symbols...>(begin, end), nullptr) << "size: " << size;

        if (size == 0)
        {
            ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), end);
            ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), nullptr);
            continue;
        }

        if (size >= 32 && !contains_null)
        {
            haystack.back() = '\0';
            ASSERT_EQ(find_first_symbols<symbols...>(begin, end), end);
            ASSERT_EQ(find_first_symbols_or_null<symbols...>(begin, end), nullptr);
            haystack.assign(size, non_needle);
            begin = haystack.data();
            end = begin + haystack.size();
        }

        for (const size_t position : positions)
        {
            if (position >= size)
                continue;

            haystack.assign(size, non_needle);
            haystack[position] = needles[position % needles.size()];
            begin = haystack.data();
            end = begin + haystack.size();

            ASSERT_EQ(find_first_symbols<symbols...>(begin, end), begin + position) << "size: " << size << ", position: " << position;
            ASSERT_EQ(find_first_symbols_or_null<symbols...>(begin, end), begin + position) << "size: " << size << ", position: " << position;
        }

        if (size >= 1024)
        {
            haystack.assign(size, non_needle);
            haystack.back() = needles[0];
            begin = haystack.data();
            end = begin + haystack.size();
            ASSERT_EQ(find_first_symbols<symbols...>(begin, end), end - 1) << "size: " << size;
            ASSERT_EQ(find_first_symbols_or_null<symbols...>(begin, end), end - 1) << "size: " << size;
        }

        haystack.assign(size, needles[0]);
        begin = haystack.data();
        end = begin + haystack.size();
        ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), end) << "size: " << size;
        ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), nullptr) << "size: " << size;

        if (size >= 32 && !contains_null)
        {
            haystack.back() = '\0';
            ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), begin + size - 1) << "size: " << size;
            ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), begin + size - 1) << "size: " << size;
            haystack.assign(size, needles[0]);
            begin = haystack.data();
            end = begin + haystack.size();
        }

        for (const size_t position : positions)
        {
            if (position >= size)
                continue;

            haystack[position] = non_needle;
            ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), begin + position) << "size: " << size << ", position: " << position;
            ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), begin + position) << "size: " << size << ", position: " << position;
            haystack[position] = needles[0];
        }

        if (size >= 1024)
        {
            haystack.assign(size, needles[0]);
            haystack.back() = non_needle;
            begin = haystack.data();
            end = begin + haystack.size();
            ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), end - 1) << "size: " << size;
            ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), end - 1) << "size: " << size;
        }
    }
}

template <char... symbols>
static void test_compile_time_randomized()
{
    const std::array<char, sizeof...(symbols)> needles {symbols...};
    constexpr std::array<size_t, 47> sizes {
        0, 1,
        15, 16, 17,
        31, 32, 33,
        47, 48, 49,
        63, 64, 65,
        95, 96,
        127, 128, 129,
        255, 256,
        511, 512, 513,
        767, 768, 769,
        1023, 1024, 1025,
        1039, 1040, 1041,
        1055, 1056, 1057,
        1087, 1088, 1089,
        1119, 1120, 1121,
        1151, 1152, 1153,
        1536, 1537,
    };
    std::uint32_t state = 0x12345678;

    for (size_t iteration = 0; iteration < 64; ++iteration)
    {
        for (const size_t size : sizes)
        {
            std::string haystack(size, '\0');
            for (char & byte : haystack)
            {
                state = state * 1664525u + 1013904223u;
                byte = static_cast<char>(state >> 24);
            }

            const char * begin = haystack.data();
            const char * end = begin + haystack.size();
            const auto expected = [&](const bool positive)
            {
                for (size_t i = 0; i < haystack.size(); ++i)
                {
                    const bool is_needle = std::find(needles.begin(), needles.end(), haystack[i]) != needles.end();
                    if (is_needle == positive)
                        return begin + i;
                }
                return end;
            };

            const char * expected_symbols = expected(true);
            const char * expected_not_symbols = expected(false);
            EXPECT_EQ(find_first_symbols<symbols...>(begin, end), expected_symbols);
            EXPECT_EQ(find_first_symbols_or_null<symbols...>(begin, end), expected_symbols == end ? nullptr : expected_symbols);
            EXPECT_EQ(find_first_not_symbols<symbols...>(begin, end), expected_not_symbols);
            EXPECT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), expected_not_symbols == end ? nullptr : expected_not_symbols);
        }
    }
}


TEST(FindSymbols, CompileTimeBoundaries)
{
    test_compile_time_boundaries<'\n'>();
    test_compile_time_boundaries<'\n', '\r'>();
    test_compile_time_boundaries<'\n', '\r', '\\'>();
    test_compile_time_boundaries<'\n', '\r', '\\', '"'>();
    test_compile_time_boundaries<'\0'>();
    test_compile_time_boundaries<'\0', '\n'>();
}

TEST(FindSymbols, CompileTimeRandomized)
{
    test_compile_time_randomized<'\n'>();
    test_compile_time_randomized<'\n', '\r'>();
    test_compile_time_randomized<'\n', '\r', '\\'>();
    test_compile_time_randomized<'\n', '\r', '\\', '"'>();
    test_compile_time_randomized<'\0'>();
    test_compile_time_randomized<'\0', '\n'>();
}

template <char... symbols>
static void test_compile_time_match_order()
{
    const std::array<char, sizeof...(symbols)> needles {symbols...};
    for (size_t alignment = 0; alignment < 32; ++alignment)
    {
        for (bool positive : {false, true})
        {
            const char fill = positive ? 'x' : needles[0];
            std::string haystack(1153 + alignment, fill);
            char * begin = haystack.data() + alignment;
            const char * end = begin + 1153;

            /// Exercise every lane after the SSE prefix, with a second match
            /// in a later vector. The combined mask must not reorder matches.
            for (size_t position = 512; position < 640; ++position)
            {
                const char match = positive ? needles[position % needles.size()] : 'x';
                begin[position] = match;
                begin[position + 32] = match;
                if (positive)
                {
                    ASSERT_EQ(find_first_symbols<symbols...>(begin, end), begin + position);
                    ASSERT_EQ(find_first_symbols_or_null<symbols...>(begin, end), begin + position);
                }
                else
                {
                    ASSERT_EQ(find_first_not_symbols<symbols...>(begin, end), begin + position);
                    ASSERT_EQ(find_first_not_symbols_or_null<symbols...>(begin, end), begin + position);
                }
                begin[position] = fill;
                begin[position + 32] = fill;
            }
        }
    }
}

TEST(FindSymbols, CompileTimeMatchOrder)
{
    test_compile_time_match_order<'\n'>();
    test_compile_time_match_order<'\n', '\r'>();
    test_compile_time_match_order<'\n', '\r', '\\'>();
    test_compile_time_match_order<'\n', '\r', '\\', '"'>();
    test_compile_time_match_order<'\0'>();
    test_compile_time_match_order<'\0', '\n', '\x80', '\xff'>();
}

TEST(FindSymbols, EmptyRange)
{
    const std::string storage = "a";
    for (const auto haystack : {std::string_view{}, std::string_view(storage).substr(0, 0)})
    {
        const char * begin = haystack.data();
        EXPECT_EQ(find_first_symbols<'a'>(begin, begin), begin);
        EXPECT_EQ(find_first_not_symbols<'a'>(begin, begin), begin);
        EXPECT_EQ(find_first_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_first_not_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_last_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_last_not_symbols_or_null<'a'>(begin, begin), nullptr);
    }
}

TEST(FindSymbols, ReversedRange)
{
    const std::array<char, 1> haystack {'a'};
    const char * begin = haystack.data() + haystack.size();
    const char * end = haystack.data();

    ASSERT_EQ(find_first_symbols<'a'>(begin, end), end);
    ASSERT_EQ(find_first_symbols_or_null<'a'>(begin, end), nullptr);
    ASSERT_EQ(find_first_not_symbols<'a'>(begin, end), end);
    ASSERT_EQ(find_first_not_symbols_or_null<'a'>(begin, end), nullptr);
    ASSERT_EQ(find_last_symbols_or_null<'a'>(begin, end), nullptr);
    ASSERT_EQ(find_last_not_symbols_or_null<'a'>(begin, end), nullptr);
}


TEST(FindSymbols, SimpleTest)
{
    const std::string s = "Hello, world! Goodbye...";
    const char * begin = s.data();
    const char * end = s.data() + s.size();

    ASSERT_EQ(find_first_symbols<'a'>(begin, end), end);
    ASSERT_EQ(find_first_symbols<'e'>(begin, end), begin + 1);
    ASSERT_EQ(find_first_symbols<'.'>(begin, end), begin + 21);
    ASSERT_EQ(find_first_symbols<' '>(begin, end), begin + 6);
    ASSERT_EQ(find_first_symbols<'H'>(begin, end), begin);
    ASSERT_EQ((find_first_symbols<'a', 'e'>(begin, end)), begin + 1);

    ASSERT_EQ((find_first_symbols<'a', 'e', 'w', 'x', 'z'>(begin, end)), begin + 1);
    ASSERT_EQ((find_first_symbols<'p', 'q', 's', 'x', 'z'>(begin, end)), end);

    ASSERT_EQ(find_last_symbols_or_null<'a'>(begin, end), nullptr);
    ASSERT_EQ(find_last_symbols_or_null<'e'>(begin, end), end - 4);
    ASSERT_EQ(find_last_symbols_or_null<'.'>(begin, end), end - 1);
    ASSERT_EQ(find_last_symbols_or_null<' '>(begin, end), end - 11);
    ASSERT_EQ(find_last_symbols_or_null<'H'>(begin, end), begin);
    ASSERT_EQ((find_last_symbols_or_null<'a', 'e'>(begin, end)), end - 4);

    {
        std::vector<std::string> vals;
        splitInto<' ', ','>(vals, "hello, world", true);
        ASSERT_EQ(vals, (std::vector<std::string>{"hello", "world"}));
    }

    {
        std::vector<std::string> vals;
        splitInto<' ', ','>(vals, "s String", true);
        ASSERT_EQ(vals, (std::vector<std::string>{"s", "String"}));
    }
}

TEST(FindNotSymbols, AllSymbolsPresent)
{
    std::string str_with_17_bytes = "hello world hello";
    std::string str_with_16_bytes = {str_with_17_bytes.begin(), str_with_17_bytes.end() - 1u};
    std::string str_with_15_bytes = {str_with_16_bytes.begin(), str_with_16_bytes.end() - 1u};

    /*
     * The below variations will choose different implementation strategies:
     * 1. Loop method only because it does not contain enough bytes for SSE 4.2
     * 2. SSE4.2 only since string contains exactly 16 bytes
     * 3. SSE4.2 + Loop method will take place because only first 16 bytes are treated by SSE 4.2 and remaining bytes is treated by loop
     *
     * Below code asserts that all calls return the ::end of the input string. This was not true prior to this fix as mentioned in PR #47304
     * */

    test_find_first_not<'h', 'e', 'l', 'o', 'w', 'r', 'd', ' '>(str_with_15_bytes, str_with_15_bytes.size());
    test_find_first_not<'h', 'e', 'l', 'o', 'w', 'r', 'd', ' '>(str_with_16_bytes, str_with_16_bytes.size());
    test_find_first_not<'h', 'e', 'l', 'o', 'w', 'r', 'd', ' '>(str_with_17_bytes, str_with_17_bytes.size());
}

TEST(FindNotSymbols, NoSymbolsMatch)
{
    std::string s = "abcdefg";

    // begin should be returned since the first character of the string does not match any of the below symbols
    test_find_first_not<'h', 'i', 'j'>(s, 0u);
}

TEST(FindNotSymbols, ExtraSymbols)
{
    std::string s = "hello_world_hello";
    test_find_first_not<'h', 'e', 'l', 'o', ' '>(s, 5u);
}

TEST(FindNotSymbols, EmptyString)
{
    std::string s;
    test_find_first_not<'h', 'e', 'l', 'o', 'w', 'r', 'd', ' '>(s, s.size());
}

TEST(FindNotSymbols, SingleChar)
{
    std::string s = "a";
    test_find_first_not<'a'>(s, s.size());
}

TEST(FindNotSymbols, NullCharacter)
{
    // special test to ensure only the passed template arguments are used as needles
    // since current find_first_symbols implementation takes in 16 characters and defaults
    // to \0.
    std::string s("abcdefg\0x", 9u);
    test_find_first_not<'a', 'b', 'c', 'd', 'e', 'f', 'g'>(s, 7u);

    // Same check with a haystack long enough to exercise the SIMD body — guards against
    // implementations that pad unused needle slots with \0 and falsely match it.
    std::string long_s("abcdefgabcdefgab\0", 17u);
    test_find_first_not<'a', 'b', 'c', 'd', 'e', 'f', 'g'>(long_s, 16u);
}
