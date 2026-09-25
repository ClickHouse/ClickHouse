#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
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

static void test_find_first_not(const std::string & haystack, const std::string & symbols, const std::size_t expected_pos)
{
    const char * begin = haystack.data();

    ASSERT_EQ(begin + expected_pos, find_first_not_symbols(haystack, SearchSymbols(symbols)));
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
    for (const auto haystack : {std::string_view{}, std::string_view(storage.data(), 0)})
    {
        const char * begin = haystack.data();
        EXPECT_EQ(find_first_symbols<'a'>(begin, begin), begin);
        EXPECT_EQ(find_first_not_symbols<'a'>(begin, begin), begin);
        EXPECT_EQ(find_first_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_first_not_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_last_symbols_or_null<'a'>(begin, begin), nullptr);
        EXPECT_EQ(find_last_not_symbols_or_null<'a'>(begin, begin), nullptr);

        for (const auto & needle : {std::string{}, std::string("a"), std::string("abcde")})
        {
            const SearchSymbols symbols(needle);
            EXPECT_EQ(find_first_symbols(haystack, symbols), begin);
            EXPECT_EQ(find_first_not_symbols(haystack, symbols), begin);
            EXPECT_EQ(find_first_symbols_or_null(haystack, symbols), nullptr);
            EXPECT_EQ(find_first_not_symbols_or_null(haystack, symbols), nullptr);
            EXPECT_EQ(find_last_symbols_or_null(haystack, symbols), nullptr);
            EXPECT_EQ(find_last_not_symbols_or_null(haystack, symbols), nullptr);
        }
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

TEST(FindSymbols, RunTimeNeedle)
{
    auto test_haystack = [](const auto & haystack, const auto & unfindable_needle) {
#define TEST_HAYSTACK_AND_NEEDLE(haystack_, needle_) \
        do { \
            const auto & h = haystack_; \
            const auto & n = needle_; \
            EXPECT_EQ( \
                    std::find_first_of(h.data(), h.data() + h.size(), n.data(), n.data() + n.size()), \
                    find_first_symbols(h, SearchSymbols(n)) \
            ) << "haystack: \"" << h << "\" (" << static_cast<const void*>(h.data()) << ")" \
              << ", needle: \"" << n << "\""; \
        } \
        while (false)

        // can't find needle
        TEST_HAYSTACK_AND_NEEDLE(haystack, unfindable_needle);

#define TEST_WITH_MODIFIED_NEEDLE(haystack, in_needle, needle_update_statement) \
        do \
        { \
            std::string needle = (in_needle); \
            (needle_update_statement); \
            TEST_HAYSTACK_AND_NEEDLE(haystack, needle); \
        } \
        while (false)

        // findable symbol is at beginning of the needle
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.front() = haystack.front());
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.front() = haystack.back());
        // Can find in the middle of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.front() = haystack[haystack.size() / 2]);

        // findable symbol is at end of the needle
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.back() = haystack.front());
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.back() = haystack.back());
        // Can find in the middle of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle.back() = haystack[haystack.size() / 2]);

        // findable symbol is in the middle of the needle
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle[needle.size() / 2] = haystack.front());
        // Can find at first pos of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle[needle.size() / 2] = haystack.back());
        // Can find in the middle of haystack
        TEST_WITH_MODIFIED_NEEDLE(haystack, unfindable_needle, needle[needle.size() / 2] = haystack[haystack.size() / 2]);

#undef TEST_WITH_MODIFIED_NEEDLE
#undef TEST_HAYSTACK_AND_NEEDLE
    };

    // there are 4 major groups of cases:
    // haystack < 16 bytes, haystack > 16 bytes
    // needle < 5 bytes,    needle >= 5 bytes

    // First and last symbols of haystack should be unique
    const std::string long_haystack = "Hello, world! Goodbye...?";
    const std::string short_haystack = "Hello, world!";

    // In sync with find_first_symbols_dispatch code: long needles receive special treatment.
    // as of now "long" means >= 5
    const std::string unfindable_long_needle = "0123456789ABCDEF";
    const std::string unfindable_short_needle = "0123";

    {
        SCOPED_TRACE("Long haystack");
        test_haystack(long_haystack, unfindable_long_needle);
        test_haystack(long_haystack, unfindable_short_needle);
    }

    {
        SCOPED_TRACE("Short haystack");
        test_haystack(short_haystack, unfindable_long_needle);
        test_haystack(short_haystack, unfindable_short_needle);
    }

    // SearchSymbols rejects needles longer than SearchSymbols::BUFFER_SIZE on every platform,
    // not only where the SSE4.2 path is compiled in.
    const std::string excessively_long_needle = "ABCDEFIJKLMNOPQRSTUVWXYZacfghijkmnpqstuvxz";
    ASSERT_ANY_THROW(SearchSymbols{excessively_long_needle});
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

    const auto * symbols = "helowrd ";

    test_find_first_not(str_with_15_bytes, symbols, str_with_15_bytes.size());
    test_find_first_not(str_with_16_bytes, symbols, str_with_16_bytes.size());
    test_find_first_not(str_with_17_bytes, symbols, str_with_17_bytes.size());
}

TEST(FindNotSymbols, NoSymbolsMatch)
{
    std::string s = "abcdefg";

    // begin should be returned since the first character of the string does not match any of the below symbols
    test_find_first_not<'h', 'i', 'j'>(s, 0u);
    test_find_first_not(s, "hij", 0u);
}

TEST(FindNotSymbols, ExtraSymbols)
{
    std::string s = "hello_world_hello";
    test_find_first_not<'h', 'e', 'l', 'o', ' '>(s, 5u);
    test_find_first_not(s, "helo ", 5u);
}

TEST(FindNotSymbols, EmptyString)
{
    std::string s;
    test_find_first_not<'h', 'e', 'l', 'o', 'w', 'r', 'd', ' '>(s, s.size());
    test_find_first_not(s, "helowrd ", s.size());
}

TEST(FindNotSymbols, SingleChar)
{
    std::string s = "a";
    test_find_first_not<'a'>(s, s.size());
    test_find_first_not(s, "a", s.size());
}

TEST(FindNotSymbols, NullCharacter)
{
    // special test to ensure only the passed template arguments are used as needles
    // since current find_first_symbols implementation takes in 16 characters and defaults
    // to \0.
    std::string s("abcdefg\0x", 9u);
    test_find_first_not<'a', 'b', 'c', 'd', 'e', 'f', 'g'>(s, 7u);
    test_find_first_not(s, "abcdefg", 7u);

    // Same check with a haystack long enough to exercise the SIMD body — guards against
    // implementations that pad unused needle slots with \0 and falsely match it.
    std::string long_s("abcdefgabcdefgab\0", 17u);
    test_find_first_not<'a', 'b', 'c', 'd', 'e', 'f', 'g'>(long_s, 16u);
    test_find_first_not(long_s, "abcdefg", 16u);
}

TEST(FindSymbols, EmptyRunTimeNeedle)
{
    // Empty SearchSymbols. With `positive=true` the result must be end/nullptr
    // (no byte is in the empty symbol set); with `positive=false` the first/last
    // byte qualifies. Long haystacks (>= 16 bytes) and embedded `\0` bytes also
    // guard against the SIMD body matching `\0` via a zero-padded needle vector.

    const SearchSymbols empty{};

    auto end_of = [](const std::string & h) { return h.data() + h.size(); };

    // Short haystack, no `\0`.
    {
        const std::string h = "abc";
        EXPECT_EQ(find_first_symbols(h, empty), end_of(h));
        EXPECT_EQ(find_first_not_symbols(h, empty), h.data());
        EXPECT_EQ(find_first_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_first_not_symbols_or_null(h, empty), h.data());
        EXPECT_EQ(find_last_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_last_not_symbols_or_null(h, empty), end_of(h) - 1);
    }

    // Empty haystack.
    {
        const std::string h;
        EXPECT_EQ(find_first_symbols(h, empty), end_of(h));
        EXPECT_EQ(find_first_not_symbols(h, empty), end_of(h));
        EXPECT_EQ(find_first_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_first_not_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_last_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_last_not_symbols_or_null(h, empty), nullptr);
    }

    // Long haystack (exercises SIMD body) containing a `\0`. A zero-padded needle
    // vector would falsely match the `\0`; the empty-needle fast path must skip
    // SIMD entirely.
    {
        const std::string h("aaaaaaaaaaaaaaaa\0aaaaaaaaaaaaaaa", 32u);
        ASSERT_EQ(h.size(), 32u);
        EXPECT_EQ(find_first_symbols(h, empty), end_of(h));
        EXPECT_EQ(find_first_symbols_or_null(h, empty), nullptr);
        EXPECT_EQ(find_last_symbols_or_null(h, empty), nullptr);

        EXPECT_EQ(find_first_not_symbols(h, empty), h.data());
        EXPECT_EQ(find_first_not_symbols_or_null(h, empty), h.data());
        EXPECT_EQ(find_last_not_symbols_or_null(h, empty), end_of(h) - 1);
    }
}

TEST(FindLastSymbols, RunTimeNeedleLongHaystack)
{
    // These exercise find_last_symbols_or_null(string_view, SearchSymbols) and
    // find_last_not_symbols_or_null(string_view, SearchSymbols) on haystacks
    // long enough to enter the SIMD reverse-search body (end - begin >= 16).
    // The reverse path computes the match index as `__builtin_clzll(bit_mask) >> 2`
    // on AArch64, so we cover the boundary positions and the \0 case.

    auto offset = [](const char * p, const std::string & h) -> ssize_t
    {
        return p == nullptr ? -1 : p - h.data();
    };

    // 32 bytes — two full SIMD chunks, no scalar tail.
    {
        const std::string haystack = "abcdefghijklmnop0123456789ABCDEF";
        ASSERT_EQ(haystack.size(), 32u);

        // Match in the second (most-recent, scanned-first) SIMD chunk.
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("F")), haystack), 31);
        // Match only in the first SIMD chunk (must skip the empty second chunk).
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("a")), haystack), 0);
        // Match in the middle of the haystack (first byte of the second chunk).
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("0")), haystack), 16);
        // Last byte of the first chunk (boundary).
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("p")), haystack), 15);
        // Multiple matches: must return the rightmost one (second chunk, position 22).
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("ag6")), haystack), 22);
        // No match — must return nullptr.
        EXPECT_EQ(find_last_symbols_or_null(haystack, SearchSymbols("xyz")), nullptr);
    }

    // 17 bytes — minimal length that takes the SIMD branch (one chunk + 1-byte scalar tail).
    {
        const std::string haystack = "0123456789abcdefX";
        ASSERT_EQ(haystack.size(), 17u);

        // Tail byte (handled by scalar prologue of the reverse helper, pos[-1]).
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("X")), haystack), 16);
        // First byte of the haystack.
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("0")), haystack), 0);
        // Match inside the SIMD chunk, not in the prologue.
        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols("5")), haystack), 5);
    }

    // \0 byte in the haystack: a runtime needle of "\0" must find it; an unrelated
    // needle must not falsely match the embedded \0. This guards against any padding
    // of unused needle slots with \0.
    {
        const std::string haystack("abcdefghijklmnop\0qrstuvwxyz", 27u);
        ASSERT_EQ(haystack.size(), 27u);

        EXPECT_EQ(offset(find_last_symbols_or_null(haystack, SearchSymbols(std::string("\0", 1u))), haystack), 16);
        EXPECT_EQ(find_last_symbols_or_null(haystack, SearchSymbols("0123")), nullptr);
    }

    // Negative variant (find_last_not_symbols_or_null) with a runtime needle.
    {
        // 17 bytes, last byte differs from the needle — that byte should be returned
        // by the scalar prologue (pos[-1]).
        const std::string haystack = "aaaaaaaaaaaaaaaab";
        ASSERT_EQ(haystack.size(), 17u);
        EXPECT_EQ(offset(find_last_not_symbols_or_null(haystack, SearchSymbols("a")), haystack), 16);

        // First byte differs from the needle, rest matches the needle — must be found
        // in the SIMD body of the reverse pass.
        const std::string haystack2 = "Xaaaaaaaaaaaaaaaa";
        ASSERT_EQ(haystack2.size(), 17u);
        EXPECT_EQ(offset(find_last_not_symbols_or_null(haystack2, SearchSymbols("a")), haystack2), 0);

        // 32 bytes with a single non-needle byte exactly at the SIMD chunk boundary
        // (position 15 — last byte of the first reverse-scanned chunk).
        const std::string haystack3 = "aaaaaaaaaaaaaaaXaaaaaaaaaaaaaaaa";
        ASSERT_EQ(haystack3.size(), 32u);
        EXPECT_EQ(offset(find_last_not_symbols_or_null(haystack3, SearchSymbols("a")), haystack3), 15);

        // All bytes match the needle — must return nullptr.
        const std::string haystack4(32u, 'a');
        EXPECT_EQ(find_last_not_symbols_or_null(haystack4, SearchSymbols("a")), nullptr);

        // \0 byte: with needle "a", an embedded \0 is a non-needle byte and must be
        // found by find_last_not.
        const std::string haystack5("aaaaaaaaaaaaaaaa\0aaaaaaaaaaaaaaa", 32u);
        ASSERT_EQ(haystack5.size(), 32u);
        EXPECT_EQ(offset(find_last_not_symbols_or_null(haystack5, SearchSymbols("a")), haystack5), 16);
    }
}
