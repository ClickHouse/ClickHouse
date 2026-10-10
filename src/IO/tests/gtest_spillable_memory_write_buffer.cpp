#include <gtest/gtest.h>

#include <base/types.h>
#include <Common/filesystemHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/SpillableMemoryWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>

#include <memory>
#include <string>

using namespace DB;

namespace
{

String makeData(size_t size)
{
    String data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>((i * 31 + 7) % 251);
    return data;
}

String readAll(ReadBuffer & buffer)
{
    String result;
    char chunk[4096];
    while (!buffer.eof())
    {
        size_t n = buffer.read(chunk, sizeof(chunk));
        if (n)
            result.append(chunk, n);
    }
    return result;
}

}

TEST(SpillableMemoryWriteBuffer, NoSpillReadBack)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;
    int read_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        ++read_creations;
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        1 << 20,
        write_creator,
        read_creator,
        []() {},
        []() {});

    String expected = makeData(1000);
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path());
        buf.write(expected.data(), expected.size());

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    /// The capacity was never reached, so nothing was spilled.
    EXPECT_EQ(0, write_creations);
    EXPECT_EQ(0, read_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, NoSpillWithoutConfig)
{
    /// A null config disables spilling: all data stays in memory even if it is large.
    String expected = makeData(100000);
    {
        SpillableMemoryWriteBuffer buf(nullptr, "/tmp/never_spill.tmp", /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 2.0, /*max_chunk_size=*/ 4096);
        buf.write(expected.data(), expected.size());

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }
}

TEST(SpillableMemoryWriteBuffer, SpillReadBack)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;
    int read_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        ++read_creations;
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        4096,
        write_creator,
        read_creator,
        []() {},
        []() {});

    String expected = makeData(100000);
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path(), /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 2.0, /*max_chunk_size=*/ 4096);
        buf.write(expected.data(), expected.size());

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    EXPECT_EQ(1, write_creations);
    EXPECT_EQ(1, read_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, TrackerAccounting)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        2048,
        write_creator,
        read_creator,
        []() {},
        []() {});

    String expected = makeData(50000);
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path(), /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 1.0, /*max_chunk_size=*/ 1024);
        buf.write(expected.data(), expected.size());

        /// The remaining in-memory part (a fixed 1 KiB chunk) must not exceed the capacity.
        EXPECT_LE(config->checker.get(), config->checker.getMaxCapacity());
        EXPECT_GE(config->checker.get(), 0);

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    EXPECT_EQ(1, write_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, ManualSpill)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int read_creations = 0;

    auto write_creator = [path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        ++read_creations;
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        1 << 20,
        write_creator,
        read_creator,
        []() {},
        []() {});

    String first = "first-part";
    String second = "second-part";
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path());
        buf.write(first.data(), first.size());
        buf.spill();
        buf.write(second.data(), second.size());

        /// The spilled part is followed by the in-memory part.
        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(first + second, readAll(*read_buf));
    }

    EXPECT_EQ(1, read_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, MultipleManualSpills)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        2048,
        write_creator,
        read_creator,
        []() {},
        []() {});

    String expected;
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path(), /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 1.0, /*max_chunk_size=*/ 1024);
        for (int i = 0; i < 5; ++i)
        {
            String part = makeData(300);
            buf.write(part.data(), part.size());
            buf.spill();
            expected += part;
        }

        /// All parts spilled to the same file, in order.
        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    /// The spill buffer is created once and reused for every spill.
    EXPECT_EQ(1, write_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, MultipleAutomaticSpills)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        2048,
        write_creator,
        read_creator,
        []() {},
        []() {});

    /// 2 KiB capacity, written in small pieces with an effective 1 KiB chunk: the
    /// capacity is hit several times, each triggering an automatic spill.
    String expected;
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path(), /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 1.0, /*max_chunk_size=*/ 1024);
        for (int i = 0; i < 10; ++i)
        {
            String part = makeData(300);
            buf.write(part.data(), part.size());
            expected += part;
        }

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    EXPECT_EQ(1, write_creations);
    EXPECT_EQ(0, config->checker.get());
}

TEST(SpillableMemoryWriteBuffer, WriteMoreThanBufferSize)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    int write_creations = 0;

    auto write_creator = [&, path = tmp_file->path()](const String &) -> std::unique_ptr<WriteBufferFromFileBase>
    {
        ++write_creations;
        return std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, -1, nullptr, 0600);
    };
    auto read_creator = [path = tmp_file->path()](const String &) -> std::unique_ptr<ReadBuffer>
    {
        return std::make_unique<ReadBufferFromFile>(path);
    };
    auto config = std::make_shared<SpillableMemoryWriteBuffer::SpillConfig>(
        4096,
        write_creator,
        read_creator,
        []() {},
        []() {});

    /// 1 MiB written with a 4 KiB capacity triggers many spills in a single write.
    String expected = makeData(1 << 20);
    {
        SpillableMemoryWriteBuffer buf(config, tmp_file->path(), /*initial_chunk_size=*/ 1024, /*growth_rate=*/ 2.0, /*max_chunk_size=*/ 4096);
        buf.write(expected.data(), expected.size());

        auto read_buf = buf.tryGetReadBuffer();
        ASSERT_TRUE(read_buf != nullptr);
        EXPECT_EQ(expected, readAll(*read_buf));
    }

    EXPECT_EQ(1, write_creations);
    EXPECT_EQ(0, config->checker.get());
}
