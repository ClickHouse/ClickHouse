/// Compares `formatV` / `strToUIntMax` with `malloc_snprintf` / `malloc_strtoumax` from the reference jemalloc.

#include <allocator/Format.h>

#include "Test.h"

#include <cerrno>
#include <climits>
#include <cstring>
#include <random>

extern "C" {
size_t malloc_snprintf(char * str, size_t size, const char * format, ...);
uintmax_t malloc_strtoumax(const char * str, char ** end_ptr, int base);
}

using namespace jemalloc;

namespace
{

size_t compared = 0;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wformat-security"

template <typename... Args>
void compareFormat(const char * format_string, Args... args)
{
    static constexpr size_t sizes[] = {1, 2, 5, 13, 64, 300};
    for (size_t size : sizes)
    {
        char expected[512];
        char actual[512];
        std::memset(expected, 'Z', sizeof(expected));
        std::memset(actual, 'Z', sizeof(actual));
        size_t expected_result = malloc_snprintf(expected, size, format_string, args...);
        size_t actual_result = format(actual, size, format_string, args...);
        ++compared;
        if (expected_result != actual_result || std::memcmp(expected, actual, sizeof(expected)) != 0)
        {
            std::fprintf(
                stderr,
                "Mismatch for format \"%s\", size %zu: \"%s\" (%zu) vs \"%s\" (%zu)\n",
                format_string,
                size,
                expected,
                expected_result,
                actual,
                actual_result);
            ++allocator_test::failureCount();
            return;
        }
    }
}

#pragma clang diagnostic pop

/// Calls `compareFormat` with the optional `*` width and precision arguments before the value.
template <typename T>
void compareWithStars(const char * format_string, bool width_star, bool precision_star, int width_arg, int precision_arg, T value)
{
    if (width_star && precision_star)
        compareFormat(format_string, width_arg, precision_arg, value);
    else if (width_star)
        compareFormat(format_string, width_arg, value);
    else if (precision_star)
        compareFormat(format_string, precision_arg, value);
    else
        compareFormat(format_string, value);
}

const int64_t signed_values[]
    = {0, 1, -1, 7, -7, 42, -1234, 99999, INT_MAX, INT_MIN, INT64_MAX, INT64_MIN, 0x7fffffffffLL, -0x123456789aLL};
const uint64_t unsigned_values[] = {0, 1, 7, 8, 15, 16, 0xabcdef, 01234567, UINT_MAX, UINT64_MAX, 0x8000000000000000ULL, 1000000000000ULL};

}

TEST(FormatOracle, Conversions)
{
    const char * flag_sets[] = {"", "#", "-", " ", "+", "#-", "- ", "+ ", "-+", "#- +"};
    const char * widths[] = {"", "0", "1", "5", "05", "012", "20", "*"};
    const char * precisions[] = {"", ".", ".0", ".2", ".5", ".*"};
    const char * lengths[] = {"", "l", "ll", "q", "j", "t", "z"};
    const char conversions[] = {'d', 'i', 'o', 'u', 'x', 'X', 'c', 's', 'p', '%'};
    const int star_values[] = {-7, 0, 3, 25};

    const char * long_string = "The quick brown fox jumps over the lazy dog";

    for (const char * flags : flag_sets)
        for (const char * width : widths)
            for (const char * precision : precisions)
                for (const char * length : lengths)
                    for (char conversion : conversions)
                    {
                        bool pad_zero = width[0] == '0';
                        bool is_signed = conversion == 'd' || conversion == 'i';
                        bool is_unsigned = conversion == 'o' || conversion == 'u' || conversion == 'x' || conversion == 'X';

                        /// Constructs the reference rejects or that are debug assertions in our implementation.
                        if (is_signed && pad_zero && config::debug)
                            continue;
                        if (is_unsigned && length[0] == 't')
                            continue;
                        if ((conversion == 'c' || conversion == 's') && length[0] != '\0'
                            && (length[0] != 'l' || length[1] == 'l' || config::debug))
                            continue;
                        if ((conversion == 'p' || conversion == '%') && length[0] != '\0')
                            continue;

                        char format_string[64];
                        std::snprintf(format_string, sizeof(format_string), "<%%%s%s%s%s%c>", flags, width, precision, length, conversion);
                        bool width_star = width[0] == '*';
                        bool precision_star = std::strcmp(precision, ".*") == 0;

                        for (int width_arg : star_values)
                            for (int precision_arg : star_values)
                            {
                                if (!width_star && width_arg != star_values[0])
                                    continue;
                                if (!precision_star && precision_arg != star_values[0])
                                    continue;
                                /// A negative width from `*` turns on left justification, which is fine, but zero padding with a
                                /// negative number is a debug assertion only when `pad_zero` is set, which `*` never sets.

                                if (is_signed)
                                {
                                    for (int64_t v : signed_values)
                                    {
                                        switch (length[0])
                                        {
                                            case '\0':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, int(v));
                                                break;
                                            case 'l':
                                                if (length[1] == 'l')
                                                    compareWithStars(
                                                        format_string,
                                                        width_star,
                                                        precision_star,
                                                        width_arg,
                                                        precision_arg,
                                                        static_cast<long long>(v));
                                                else
                                                    compareWithStars(
                                                        format_string, width_star, precision_star, width_arg, precision_arg, long(v));
                                                break;
                                            case 'q':
                                                compareWithStars(
                                                    format_string,
                                                    width_star,
                                                    precision_star,
                                                    width_arg,
                                                    precision_arg,
                                                    static_cast<long long>(v));
                                                break;
                                            case 'j':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, intmax_t(v));
                                                break;
                                            case 't':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, ptrdiff_t(v));
                                                break;
                                            case 'z':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, ssize_t(v));
                                                break;
                                        }
                                    }
                                }
                                else if (is_unsigned)
                                {
                                    for (uint64_t v : unsigned_values)
                                    {
                                        switch (length[0])
                                        {
                                            case '\0':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, unsigned(v));
                                                break;
                                            case 'l':
                                                if (length[1] == 'l')
                                                    compareWithStars(
                                                        format_string,
                                                        width_star,
                                                        precision_star,
                                                        width_arg,
                                                        precision_arg,
                                                        static_cast<unsigned long long>(v));
                                                else
                                                    compareWithStars(
                                                        format_string,
                                                        width_star,
                                                        precision_star,
                                                        width_arg,
                                                        precision_arg,
                                                        static_cast<unsigned long>(v));
                                                break;
                                            case 'q':
                                                compareWithStars(
                                                    format_string,
                                                    width_star,
                                                    precision_star,
                                                    width_arg,
                                                    precision_arg,
                                                    static_cast<unsigned long long>(v));
                                                break;
                                            case 'j':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, uintmax_t(v));
                                                break;
                                            case 'z':
                                                compareWithStars(
                                                    format_string, width_star, precision_star, width_arg, precision_arg, size_t(v));
                                                break;
                                        }
                                    }
                                }
                                else if (conversion == 'c')
                                {
                                    for (int v : {int('a'), int('Z'), int(' '), 0x7f, 0x1ff})
                                        compareWithStars(format_string, width_star, precision_star, width_arg, precision_arg, v);
                                }
                                else if (conversion == 's')
                                {
                                    /// With a precision, exactly that many bytes are copied, so only use strings that are long enough.
                                    for (const char * v : {"", "x", "hello", long_string})
                                    {
                                        bool has_precision = precision[0] == '.';
                                        int precision_value
                                            = precision_star ? precision_arg : (precision[1] ? std::atoi(precision + 1) : 0);
                                        if (has_precision && precision_value >= 0 && size_t(precision_value) > std::strlen(v))
                                            continue;
                                        compareWithStars(format_string, width_star, precision_star, width_arg, precision_arg, v);
                                    }
                                }
                                else if (conversion == 'p')
                                {
                                    for (uintptr_t v : {uintptr_t(0), uintptr_t(0x42), uintptr_t(0xffff800012345678ULL), UINTPTR_MAX})
                                        compareWithStars(
                                            format_string,
                                            width_star,
                                            precision_star,
                                            width_arg,
                                            precision_arg,
                                            reinterpret_cast<void *>(v));
                                }
                                else
                                {
                                    /// "%%" takes no argument; `*` still consumes ints.
                                    if (width_star && precision_star)
                                        compareFormat(format_string, width_arg, precision_arg);
                                    else if (width_star)
                                        compareFormat(format_string, width_arg);
                                    else if (precision_star)
                                        compareFormat(format_string, precision_arg);
                                    else
                                        compareFormat(format_string);
                                }
                            }
                    }
    std::fprintf(stderr, "compared %zu outputs\n", compared);
    CHECK_GT(compared, 100000u);
}

TEST(FormatOracle, Mixed)
{
    compareFormat("plain text without conversions");
    compareFormat("");
    compareFormat("%s:%d: %s %zu %p%%\n", "file.c", 123, "message", size_t(4096), reinterpret_cast<void *>(0x7f0000001000));
    compareFormat(
        "%" FORMAT_U64 " %" FORMAT_D64 " %#" FORMAT_X64 " %" FORMAT_U32 " %" FORMAT_XPTR,
        uint64_t(1) << 63,
        int64_t(-5),
        uint64_t(0),
        uint32_t(7),
        uintptr_t(0xdead));
    compareFormat("%-20s|%20s|%-5u|%5u|", "left", "right", 1u, 2u);
    compareFormat("%c%c%c", 'a', 'b', 'c');
    compareFormat(
        "%s",
        "a very long string that is longer than most of the buffer sizes used by the comparison, "
        "to exercise truncation in the middle of a %s conversion");
}

TEST(FormatOracle, StrToUMax)
{
    const char alphabet[] = "0123456789abcdefgxXzZ+- \t\n_.";
    const int bases[] = {-1, 0, 1, 2, 7, 8, 10, 16, 35, 36, 37};
    std::mt19937_64 rng(12345);

    auto compare = [&](const char * input, int base)
    {
        errno = 0;
        char * expected_end = nullptr;
        uintmax_t expected = malloc_strtoumax(input, &expected_end, base);
        int expected_errno = errno;

        errno = 0;
        char * actual_end = nullptr;
        uintmax_t actual = strToUIntMax(input, &actual_end, base);
        int actual_errno = errno;

        if (expected != actual || expected_errno != actual_errno || expected_end != actual_end)
        {
            std::fprintf(
                stderr,
                "Mismatch for \"%s\" base %d: %ju errno %d end +%td vs %ju errno %d end +%td\n",
                input,
                base,
                expected,
                expected_errno,
                expected_end - input,
                actual,
                actual_errno,
                actual_end - input);
            ++allocator_test::failureCount();
        }
    };

    const char * fixed[]
        = {"0",
           "00",
           "08",
           "0x",
           "0x1",
           "0X1f",
           "-0",
           "+0",
           " 0",
           "0x0x",
           "18446744073709551615",
           "18446744073709551616",
           "36893488147419103232",
           "340282366920938463463374607431768211456",
           "zzzzzzzzzzzzzzzz",
           "ffffffffffffffff",
           "10000000000000000",
           "-18446744073709551615",
           "1777777777777777777777",
           "2000000000000000000000"};
    for (const char * input : fixed)
        for (int base : bases)
            compare(input, base);

    char input[32];
    for (size_t iteration = 0; iteration < 300000; ++iteration)
    {
        size_t len = rng() % 24;
        for (size_t i = 0; i < len; ++i)
            input[i] = alphabet[rng() % (sizeof(alphabet) - 1)];
        input[len] = '\0';
        compare(input, bases[rng() % std::size(bases)]);
    }
}
