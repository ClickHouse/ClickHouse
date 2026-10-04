#include <allocator/BufferedWriter.h>
#include <allocator/Format.h>

#include "Test.h"

#include <cerrno>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

using namespace jemalloc;

/// Cases from jemalloc's `test/unit/malloc_io.c` plus the quirks of `malloc_strtoumax`.
TEST(Format, StrToUMax)
{
    struct Case
    {
        const char * input;
        const char * expected_remainder;
        int base;
        int expected_errno;
        uintmax_t expected_x;
    };
    const Case cases[] = {
        {"0", "0", -1, EINVAL, UINTMAX_MAX},
        {"0", "0", 1, EINVAL, UINTMAX_MAX},
        {"0", "0", 37, EINVAL, UINTMAX_MAX},
        {"", "", 0, EINVAL, UINTMAX_MAX},
        {"+", "+", 0, EINVAL, UINTMAX_MAX},
        {"++3", "++3", 0, EINVAL, UINTMAX_MAX},
        {"-", "-", 0, EINVAL, UINTMAX_MAX},
        {"42", "", 0, 0, 42},
        {"+42", "", 0, 0, 42},
        {"-42", "", 0, 0, uintmax_t(intmax_t(-42))},
        {"042", "", 0, 0, 042},
        {"+042", "", 0, 0, 042},
        {"-042", "", 0, 0, uintmax_t(intmax_t(-042))},
        {"0x42", "", 0, 0, 0x42},
        {"+0x42", "", 0, 0, 0x42},
        {"-0x42", "", 0, 0, uintmax_t(intmax_t(-0x42))},
        {"0", "", 0, 0, 0},
        {"1", "", 0, 0, 1},
        {" 42", "", 0, 0, 42},
        {"\t\n\v\f\r 42", "", 0, 0, 42},
        {"42 ", " ", 0, 0, 42},
        {"0x", "x", 0, 0, 0},
        {"42x", "x", 0, 0, 42},
        {"07", "", 0, 0, 7},
        {"010", "", 0, 0, 8},
        {"08", "8", 0, 0, 0},
        {"0_", "_", 0, 0, 0},
        {"0X", "X", 0, 0, 0},
        {"0xg", "xg", 0, 0, 0},
        {"0XA", "", 0, 0, 10},
        {"010", "", 10, 0, 10},
        {"0x3", "x3", 10, 0, 0},
        {"08", "8", 10, 0, 0},
        {"09", "9", 10, 0, 0},
        {"12", "2", 2, 0, 1},
        {"78", "8", 8, 0, 7},
        {"9a", "a", 10, 0, 9},
        {"9A", "A", 10, 0, 9},
        {"fg", "g", 16, 0, 15},
        {"FG", "G", 16, 0, 15},
        {"0xfg", "g", 16, 0, 15},
        {"0XFG", "G", 16, 0, 15},
        {"z_", "_", 36, 0, 35},
        {"Z_", "_", 36, 0, 35},
        {"18446744073709551615", "", 10, 0, UINTMAX_MAX},
        {"18446744073709551616", "6", 10, ERANGE, UINTMAX_MAX},
        {"99999999999999999999x", "9x", 10, ERANGE, UINTMAX_MAX},
        {"-1", "", 10, 0, UINTMAX_MAX},
        {"  -x", "  -x", 10, EINVAL, UINTMAX_MAX},
    };

    for (const auto & c : cases)
    {
        errno = 0;
        char * remainder = nullptr;
        uintmax_t result = strToUMax(c.input, &remainder, c.base);
        int err = errno;
        CHECK_EQ(err, c.expected_errno);
        CHECK_STREQ(remainder, c.expected_remainder);
        CHECK_EQ(result, c.expected_x);
    }

    errno = 0;
    CHECK_EQ(strToUMax("0", static_cast<char **>(nullptr), 0), 0u);
    CHECK_EQ(errno, 0);
}

namespace
{

char buf[128];

template <typename... Args>
void checkFormat(const char * expected, const char * fmt, Args... args)
{
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wformat-security"
    size_t result = format(buf, sizeof(buf), fmt, args...);
#pragma clang diagnostic pop
    CHECK_STREQ(buf, expected);
    CHECK_EQ(result, std::strlen(expected));
}

}

TEST(Format, Basic)
{
    checkFormat("hello", "hello");
    checkFormat("50%, 100%", "50%%, %d%%", 100);
    checkFormat("a0123b", "a%sb", "0123");
    checkFormat("a 0123b", "a%5sb", "0123");
    checkFormat("a 0123b", "a%*sb", 5, "0123");
    checkFormat("a0123 b", "a%-5sb", "0123");
    checkFormat("a0123b", "a%*sb", -1, "0123");
    checkFormat("a0123 b", "a%*sb", -5, "0123");
    checkFormat("a0123 b", "a%-*sb", -5, "0123");
    checkFormat("a012b", "a%.3sb", "0123");
    checkFormat("a012b", "a%.*sb", 3, "0123");
    checkFormat("a0123b", "a%.*sb", -3, "0123");
    checkFormat("a  012b", "a%5.3sb", "0123");
    checkFormat("a  012b", "a%5.*sb", 3, "0123");
    checkFormat("a  012b", "a%*.3sb", 5, "0123");
    checkFormat("a  012b", "a%*.*sb", 5, 3, "0123");
    checkFormat("a 0123b", "a%*.*sb", 5, -3, "0123");
    checkFormat("_abcd_", "_%x_", 0xabcd);
    checkFormat("_0xabcd_", "_%#x_", 0xabcd);
    checkFormat("_1234_", "_%o_", 01234);
    checkFormat("_01234_", "_%#o_", 01234);
    checkFormat("_0_", "_%#o_", 0);
    checkFormat("_1234_", "_%u_", 1234);
    checkFormat("01234", "%05u", 1234);
    checkFormat("_1234_", "_%d_", 1234);
    checkFormat("_ 1234_", "_% d_", 1234);
    checkFormat("_+1234_", "_%+d_", 1234);
    checkFormat("_-1234_", "_%d_", -1234);
    checkFormat("_-1234_", "_% d_", -1234);
    checkFormat("_-1234_", "_%+d_", -1234);
    checkFormat("_-1234_", "_%i_", -1234);
    checkFormat("_0x1234abc_", "_%#x_", 0x1234abc);
    checkFormat("_0X1234ABC_", "_%#X_", 0x1234abc);
    checkFormat("_c_", "_%c_", 'c');
    checkFormat("_  c_", "_%3c_", 'c');
    checkFormat("_c  _", "_%-3c_", 'c');
    checkFormat("_string_", "_%s_", "string");
    checkFormat("_0x42_", "_%p_", reinterpret_cast<void *>(0x42));
    checkFormat("_0x0_", "_%p_", static_cast<void *>(nullptr));
    checkFormat("_  0x42_", "_%6p_", reinterpret_cast<void *>(0x42));
    checkFormat("_0x0_", "_%#x_", 0);
    checkFormat("_0x0_", "_%#lx_", 0ul);
    checkFormat("_-1234_", "_%ld_", -1234l);
    checkFormat("_01234_", "_%#lo_", 01234l);
    checkFormat("_0X1234ABC_", "_%#lX_", 0x1234ABCl);
    checkFormat("_-1234_", "_%lld_", -1234ll);
    checkFormat("_0x1234abc_", "_%#llx_", 0x1234abcll);
    checkFormat("_-1234_", "_%jd_", intmax_t(-1234));
    checkFormat("_1234_", "_%ju_", uintmax_t(1234));
    checkFormat("_-1234_", "_%td_", ptrdiff_t(-1234));
    checkFormat("_-1234_", "_%zd_", ssize_t(-1234));
    checkFormat("_0x1234abc_", "_%#zx_", size_t(0x1234abc));
    checkFormat("18446744073709551615", "%" FMTu64, UINT64_MAX);
    checkFormat("-9223372036854775808", "%" FMTd64, INT64_MIN);
    checkFormat("9223372036854775807", "%" FMTd64, INT64_MAX);
    checkFormat("ffffffffffffffff", "%" FMTx64, UINT64_MAX);
    checkFormat("4294967295", "%u", UINT_MAX);
    checkFormat("-2147483648", "%d", INT_MIN);
    checkFormat("01777777777777777777777", "%" FMTx64 "%zo", uint64_t(0), SIZE_MAX);
    /// Left-justified with a zero "flag": zero padding is ignored.
    checkFormat("12   |", "%-05u|", 12u);
    /// Zero padding goes before the `0x` prefix.
    checkFormat("000x1f", "%#06x", 0x1f);
    /// Zero width.
    checkFormat("7", "%0u", 7u);
    /// The width "010" is parsed in base 10.
    checkFormat("0000000007", "%010.3u", 7u);
}

TEST(Format, Truncated)
{
    constexpr size_t BUFLEN = 15;
    char small[BUFLEN];

    for (size_t len = 1; len < BUFLEN; ++len)
    {
        size_t result = format(small, len, "012346789");
        CHECK_EQ(std::strncmp(small, "012346789", len - 1), 0);
        CHECK_EQ(std::strlen(small), minOf(len - 1, size_t(9)));
        CHECK_EQ(result, 9u);

        result = format(small, len, "a%-6s", "0123");
        CHECK_EQ(std::strncmp(small, "a0123  ", len - 1), 0);
        CHECK_EQ(result, 7u);

        result = format(small, len, "a%*.*s", 6, 3, "0123");
        CHECK_EQ(std::strncmp(small, "a   012", len - 1), 0);
        CHECK_EQ(result, 7u);

        result = format(small, len, "a% db", 123);
        CHECK_EQ(std::strncmp(small, "a 123b", len - 1), 0);
        CHECK_EQ(result, 6u);

        result = format(small, len, "a%+db", 123);
        CHECK_EQ(std::strncmp(small, "a+123b", len - 1), 0);
        CHECK_EQ(result, 6u);
    }
}

namespace
{

char captured[8192];
size_t captured_len = 0;
int captured_calls = 0;
void * captured_opaque = nullptr;

void captureCallback(void * cbopaque, const char * s)
{
    size_t len = std::strlen(s);
    std::memcpy(captured + captured_len, s, len + 1);
    captured_len += len;
    ++captured_calls;
    captured_opaque = cbopaque;
}

void resetCapture()
{
    captured[0] = '\0';
    captured_len = 0;
    captured_calls = 0;
    captured_opaque = nullptr;
}

}

TEST(Format, PrintToCallback)
{
    resetCapture();
    int opaque = 0;
    printToCallback(captureCallback, &opaque, "x=%d y=%s", 5, "abc");
    CHECK_STREQ(captured, "x=5 y=abc");
    CHECK_EQ(captured_calls, 1);
    CHECK_EQ(captured_opaque, static_cast<void *>(&opaque));

    /// Output is truncated to MALLOC_PRINTF_BUFSIZE - 1 characters.
    resetCapture();
    printToCallback(captureCallback, nullptr, "%5000s", "x");
    CHECK_EQ(captured_len, MALLOC_PRINTF_BUFSIZE - 1);
    CHECK_EQ(captured_calls, 1);

    /// With a null callback, je_malloc_message is used.
    resetCapture();
    auto * saved = je_malloc_message;
    je_malloc_message = captureCallback;
    printToCallback(nullptr, &opaque, "hello %u", 42u);
    CHECK_STREQ(captured, "hello 42");
    CHECK_EQ(captured_opaque, static_cast<void *>(&opaque));

    resetCapture();
    printMessage("%s!", "message");
    CHECK_STREQ(captured, "message!");
    CHECK_EQ(captured_opaque, static_cast<void *>(nullptr));

    resetCapture();
    writeMessage("raw");
    CHECK_STREQ(captured, "raw");
    CHECK(messageCallback() == captureCallback);
    je_malloc_message = saved;
    CHECK(messageCallback() == defaultWriteMessage);
}

TEST(Format, BufferError)
{
    char b[BUFERROR_BUF];
    CHECK_EQ(bufferError(ENOENT, b, sizeof(b)), 0);
    CHECK_STREQ(b, "No such file or directory");
    char tiny[4];
    CHECK_EQ(bufferError(ENOENT, tiny, sizeof(tiny)), 0);
    CHECK_STREQ(tiny, "No ");
}

TEST(Format, FileIO)
{
    int fds[2];
    REQUIRE(::pipe(fds) == 0);
    CHECK_EQ(writeFd(fds[1], "abcdef", 6), 6);
    CHECK_EQ(writeFd(fds[1], "", 0), 0);
    CHECK_EQ(closeFile(fds[1]), 0);
    char rb[16] = {};
    CHECK_EQ(readFd(fds[0], rb, sizeof(rb)), 6);
    CHECK_STREQ(rb, "abcdef");
    CHECK_EQ(readFd(fds[0], rb, sizeof(rb)), 0);
    CHECK_EQ(closeFile(fds[0]), 0);
    CHECK_LT(writeFd(fds[1], "x", 1), 0);

    int fd = openFile("/proc/self/stat", O_RDONLY);
    if (fd < 0)
        fd = openFile("/dev/null", O_RDONLY);
    REQUIRE(fd >= 0);
    CHECK_EQ(seekFile(fd, 0, SEEK_SET), 0);
    CHECK_EQ(closeFile(fd), 0);
    CHECK_LT(openFile("/nonexistent/file", O_RDONLY), 0);
}

namespace
{

char writer_buf[16];

struct Source
{
    const char * data;
    size_t pos;
    size_t chunk;
};

ssize_t readSource(void * opaque, void * dst, size_t limit)
{
    auto * src = static_cast<Source *>(opaque);
    size_t remaining = std::strlen(src->data) - src->pos;
    size_t n = minOf(minOf(remaining, limit), src->chunk);
    std::memcpy(dst, src->data + src->pos, n);
    src->pos += n;
    return ssize_t(n);
}

void * test_allocated = nullptr;
void * test_freed = nullptr;
char heap_buf[64];

void * testAllocate(ThreadState *, size_t size)
{
    REQUIRE(size <= sizeof(heap_buf));
    test_allocated = heap_buf;
    return heap_buf;
}

void testDeallocate(ThreadState *, void * ptr)
{
    test_freed = ptr;
}

void * failAllocate(ThreadState *, size_t)
{
    return nullptr;
}

constexpr BufferAllocator test_allocator{testAllocate, testDeallocate};
constexpr BufferAllocator failing_allocator{failAllocate, testDeallocate};

}

TEST(BufferedWriter, FlushRules)
{
    resetCapture();
    int opaque = 0;
    BufferedWriter writer;
    CHECK(!writer.init(nullptr, captureCallback, &opaque, writer_buf, sizeof(writer_buf)));

    /// 15 usable bytes: writing exactly 15 does not flush.
    writer.write("0123456789abcde");
    CHECK_EQ(captured_calls, 0);
    /// More data arrives: flush the full buffer first.
    writer.write("X");
    CHECK_EQ(captured_calls, 1);
    CHECK_STREQ(captured, "0123456789abcde");
    CHECK_EQ(captured_opaque, static_cast<void *>(&opaque));

    /// A long string is split in pieces of 15.
    writer.write("abcdefghijklmnopqrstuvwxyz0123456789");
    CHECK_EQ(captured_calls, 3);
    CHECK_STREQ(captured, "0123456789abcdeXabcdefghijklmnopqrstuvwxyz012");

    BufferedWriter::callback(&writer, "!");
    writer.terminate(nullptr);
    CHECK_STREQ(captured, "0123456789abcdeXabcdefghijklmnopqrstuvwxyz0123456789!");

    /// Terminating an empty buffer still calls back with "".
    resetCapture();
    CHECK(!writer.init(nullptr, captureCallback, nullptr, writer_buf, sizeof(writer_buf)));
    writer.terminate(nullptr);
    CHECK_EQ(captured_calls, 1);
    CHECK_STREQ(captured, "");
}

TEST(BufferedWriter, Allocated)
{
    resetCapture();
    test_allocated = nullptr;
    test_freed = nullptr;
    BufferedWriter writer;
    CHECK(!writer.init(nullptr, captureCallback, nullptr, nullptr, 32, &test_allocator));
    CHECK(test_allocated == heap_buf);
    writer.write("hello");
    writer.terminate(nullptr);
    CHECK(test_freed == heap_buf);
    CHECK_STREQ(captured, "hello");

    /// Allocation failure: writes go straight through, nothing is freed.
    resetCapture();
    test_freed = nullptr;
    CHECK(writer.init(nullptr, captureCallback, nullptr, nullptr, 32, &failing_allocator));
    CHECK(!writer.hasBuffer());
    writer.write("a");
    writer.write("b");
    CHECK_EQ(captured_calls, 2);
    writer.flush();
    CHECK_EQ(captured_calls, 2);
    writer.terminate(nullptr);
    CHECK_EQ(captured_calls, 2);
    CHECK(test_freed == nullptr);
}

TEST(BufferedWriter, Pipe)
{
    resetCapture();
    BufferedWriter writer;
    CHECK(!writer.init(nullptr, captureCallback, nullptr, writer_buf, sizeof(writer_buf)));
    Source src{"The quick brown fox jumps over the lazy dog", 0, 7};
    writer.pipe(readSource, &src);
    CHECK_STREQ(captured, "The quick brown fox jumps over the lazy dog");
    /// 43 bytes through a 15-byte buffer: flushes at 15, 30, then the final flush.
    CHECK_EQ(captured_calls, 3);

    /// Without a buffer: the 16-byte backup buffer is used.
    resetCapture();
    CHECK(writer.init(nullptr, captureCallback, nullptr, nullptr, 32, nullptr));
    Source src2{"The quick brown fox jumps over the lazy dog", 0, 100};
    writer.pipe(readSource, &src2);
    CHECK_STREQ(captured, "The quick brown fox jumps over the lazy dog");
    CHECK_EQ(captured_calls, 3);
}
