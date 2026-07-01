#include <gtest/gtest.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <filesystem>


using namespace DB;
namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

class AsynchronousBoundedReadBufferTest : public ::testing::TestWithParam<const char *>
{
public:
    AsynchronousBoundedReadBufferTest() { fs::create_directories(temp_folder.path()); }

    String makeTempFile(const String & contents)
    {
        String path = fmt::format("{}/{}", temp_folder.path(), counter);
        ++counter;

        WriteBufferFromFile out{path};
        out.write(contents.data(), contents.size());
        out.finalize();

        return path;
    }

private:
    Poco::TemporaryFile temp_folder;
    size_t counter = 0;
};

static String getAlphabetWithDigits()
{
    String contents;
    for (char c = 'a'; c <= 'z'; ++c)
        contents += c;
    for (char c = '0'; c <= '9'; ++c)
        contents += c;
    return contents;
}


TEST_F(AsynchronousBoundedReadBufferTest, setReadUntilPosition)
{
    String file_path = makeTempFile(getAlphabetWithDigits());
    ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

    for (bool with_prefetch : {false, true})
    {
        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            DBMS_DEFAULT_BUFFER_SIZE, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);
        read_buffer.setReadUntilPosition(20);

        auto try_read = [&](size_t count)
        {
            if (with_prefetch)
                read_buffer.prefetch(Priority{0});

            String str;
            str.resize(count);
            str.resize(read_buffer.read(str.data(), str.size()));
            return str;
        };

        EXPECT_EQ(try_read(15), "abcdefghijklmno");
        EXPECT_EQ(try_read(15), "pqrst");
        EXPECT_EQ(try_read(15), "");

        read_buffer.setReadUntilPosition(25);

        EXPECT_EQ(try_read(15), "uvwxy");
        EXPECT_EQ(try_read(15), "");

        read_buffer.setReadUntilEnd();

        EXPECT_EQ(try_read(15), "z0123456789");
        EXPECT_EQ(try_read(15), "");
    }
}

TEST_F(AsynchronousBoundedReadBufferTest, throwsWhenFileGrewBeyondCachedSize)
{
    /// Open a buffer over a 1-byte file, then grow the file on disk, simulating an
    /// object-storage metadata file (e.g. a Paimon LATEST snapshot hint) rewritten by
    /// a concurrent external writer after its size was cached. The cached file size
    /// stays 1 while the reader hands back the grown content, so the read must throw a
    /// catchable CANNOT_READ_ALL_DATA rather than aborting or returning a stale prefix.
    ///
    /// Both bound configurations must throw: no explicit bound (readAt-capable pipelines),
    /// and read_until_position set to the cached size by setReadUntilEnd (the bound that
    /// StorageObjectStorageSource sets before prefetch for !supportsReadAt pipelines).
    for (bool with_read_until_end : {false, true})
    {
        String file_path = makeTempFile("1");
        ThreadPoolRemoteFSReader remote_fs_reader(4, 0);

        AsynchronousBoundedReadBuffer read_buffer(
            createReadBufferFromFileBase(file_path, ReadSettings{}), remote_fs_reader,
            DBMS_DEFAULT_BUFFER_SIZE, /* min_bytes_for_seek */ 0,
            Priority{0}, /* page_cache_block_size */ 0, /* enable_prefetches_log */ false);

        if (with_read_until_end)
            read_buffer.setReadUntilEnd();

        {
            WriteBufferFromFile out{file_path};
            out.write("100", 3);
            out.finalize();
        }

        String str;
        str.resize(16);
        try
        {
            [[maybe_unused]] size_t bytes_read = read_buffer.read(str.data(), str.size());
            FAIL() << "Expected CANNOT_READ_ALL_DATA to be thrown (with_read_until_end=" << with_read_until_end << ")";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::CANNOT_READ_ALL_DATA) << "with_read_until_end=" << with_read_until_end;
        }
    }
}
