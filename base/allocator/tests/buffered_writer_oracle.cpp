/// Compares the sequence of callback invocations of `BufferedWriter` with jemalloc's `buf_writer` (static buffers).

#include <allocator/BufferedWriter.h>

#include "Test.h"

#include <cstring>
#include <random>

extern "C"
{
struct buf_writer_t
{
    void (*write_cb)(void *, const char *);
    void * cbopaque;
    char * buf;
    size_t buf_size;
    size_t buf_end;
    bool internal_buf;
};
bool buf_writer_init(void * tsdn, buf_writer_t * buf_writer, void (*write_cb)(void *, const char *), void * cbopaque, char * buf, size_t buf_len);
void buf_writer_flush(buf_writer_t * buf_writer);
void buf_writer_cb(void * buf_writer, const char * s);
void buf_writer_terminate(void * tsdn, buf_writer_t * buf_writer);
void buf_writer_pipe(buf_writer_t * buf_writer, ssize_t (*read_cb)(void *, void *, size_t), void * read_cbopaque);

/// `buf_writer.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace,
/// which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

/// Records every callback: the string and the opaque pointer, separated by '|'.
struct Log
{
    char data[1 << 16];
    size_t len = 0;
    size_t calls = 0;
};

void record(void * opaque, const char * s)
{
    auto * log = static_cast<Log *>(opaque);
    size_t n = std::strlen(s);
    REQUIRE(log->len + n + 1 < sizeof(log->data));
    std::memcpy(log->data + log->len, s, n);
    log->len += n;
    log->data[log->len++] = '|';
    ++log->calls;
}

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
    size_t n = remaining < limit ? remaining : limit;
    n = n < src->chunk ? n : src->chunk;
    std::memcpy(dst, src->data + src->pos, n);
    src->pos += n;
    return ssize_t(n);
}

}

TEST(BufferedWriterOracle, RandomWrites)
{
    std::mt19937_64 rng(99);
    char text[200];
    for (size_t i = 0; i < sizeof(text) - 1; ++i)
        text[i] = char('a' + i % 26);
    text[sizeof(text) - 1] = '\0';

    static Log expected_log;
    static Log actual_log;

    for (size_t iteration = 0; iteration < 2000; ++iteration)
    {
        size_t buf_len = 2 + rng() % 40;
        char expected_buf[64];
        char actual_buf[64];
        expected_log.len = actual_log.len = 0;
        expected_log.calls = actual_log.calls = 0;

        buf_writer_t c_writer;
        BufferedWriter writer;
        CHECK_EQ(buf_writer_init(nullptr, &c_writer, record, &expected_log, expected_buf, buf_len), false);
        CHECK_EQ(writer.init(nullptr, record, &actual_log, actual_buf, buf_len), false);

        size_t ops = rng() % 30;
        for (size_t op = 0; op < ops; ++op)
        {
            size_t kind = rng() % 10;
            if (kind == 0)
            {
                buf_writer_flush(&c_writer);
                writer.flush();
            }
            else if (kind == 1)
            {
                Source src_c{text + rng() % 100, 0, 1 + rng() % 50};
                Source src{src_c.data, 0, src_c.chunk};
                buf_writer_pipe(&c_writer, readSource, &src_c);
                writer.pipe(readSource, &src);
            }
            else
            {
                const char * s = text + (sizeof(text) - 1 - rng() % 90);
                buf_writer_cb(&c_writer, s);
                BufferedWriter::callback(&writer, s);
            }
            CHECK_EQ(actual_log.calls, expected_log.calls);
        }
        buf_writer_terminate(nullptr, &c_writer);
        writer.terminate(nullptr);

        CHECK_EQ(actual_log.calls, expected_log.calls);
        CHECK_EQ(actual_log.len, expected_log.len);
        CHECK(std::memcmp(actual_log.data, expected_log.data, expected_log.len) == 0);
    }
}
