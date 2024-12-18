#include <gtest/gtest.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <filesystem>


using namespace DB;
namespace fs = std::filesystem;

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

String getAlphabetWithDigits()
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
        AsynchronousBoundedReadBuffer read_buffer(createReadBufferFromFileBase(file_path, {}), remote_fs_reader, {}, DBMS_DEFAULT_BUFFER_SIZE);
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
