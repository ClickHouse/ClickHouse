#include <gtest/gtest.h>
#include <Common/config.h>

#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Poco/TemporaryFile.h>
#include <filesystem>


namespace DB::ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
}

namespace fs = std::filesystem;
using namespace DB;


class ArchiveReaderAndWriterTest : public ::testing::TestWithParam<const char *>
{
public:
    ArchiveReaderAndWriterTest()
    {
        const char * archive_file_ext = GetParam();
        path_to_archive = temp_folder.path() + "/archive" + archive_file_ext;
        fs::create_directories(temp_folder.path());
    }

    const String & getPathToArchive() const { return path_to_archive; }

    static void expectException(int code, const String & message, const std::function<void()> & func)
    {
        try
        {
            func();
        }
        catch (Exception & e)
        {
            if ((e.code() != code) || (e.message().find(message) == String::npos))
                throw;
        }
    }

private:
    Poco::TemporaryFile temp_folder;
    String path_to_archive;
};


TEST_P(ArchiveReaderAndWriterTest, EmptyArchive)
{
    /// Make an archive.
    {
        createArchiveWriter(getPathToArchive());
    }

    /// The created archive can be found in the local filesystem.
    ASSERT_TRUE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    EXPECT_FALSE(reader->fileExists("nofile.txt"));

    expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "File 'nofile.txt' not found",
                    [&]{ reader->getFileInfo("nofile.txt"); });

    expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "File 'nofile.txt' not found",
                    [&]{ reader->readFile("nofile.txt"); });

    EXPECT_EQ(reader->firstFile(), nullptr);
}


TEST_P(ArchiveReaderAndWriterTest, SingleFileInArchive)
{
    /// Make an archive.
    std::string_view contents = "The contents of a.txt";
    {
        auto writer = createArchiveWriter(getPathToArchive());
        {
            auto out = writer->writeFile("a.txt");
            writeString(contents, *out);
        }
    }

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    ASSERT_TRUE(reader->fileExists("a.txt"));

    auto file_info = reader->getFileInfo("a.txt");
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    EXPECT_GT(file_info.compressed_size, 0);

    {
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }

    {
        /// Use an enumerator.
        auto enumerator = reader->firstFile();
        ASSERT_NE(enumerator, nullptr);
        EXPECT_EQ(enumerator->getFileName(), "a.txt");
        EXPECT_EQ(enumerator->getFileInfo().uncompressed_size, contents.size());
        EXPECT_GT(enumerator->getFileInfo().compressed_size, 0);
        EXPECT_FALSE(enumerator->nextFile());
    }

    {
        /// Use converting an enumerator to a reading buffer and vice versa.
        auto enumerator = reader->firstFile();
        ASSERT_NE(enumerator, nullptr);
        EXPECT_EQ(enumerator->getFileName(), "a.txt");
        auto in = reader->readFile(std::move(enumerator));
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
        enumerator = reader->nextFile(std::move(in));
        EXPECT_EQ(enumerator, nullptr);
    }

    {
        /// Wrong using of an enumerator throws an exception.
        auto enumerator = reader->firstFile();
        ASSERT_NE(enumerator, nullptr);
        EXPECT_FALSE(enumerator->nextFile());
        expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "No current file",
                        [&]{ enumerator->getFileName(); });

        expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "No current file",
                        [&] { reader->readFile(std::move(enumerator)); });
    }
}


TEST_P(ArchiveReaderAndWriterTest, TwoFilesInArchive)
{
    /// Make an archive.
    std::string_view a_contents = "The contents of a.txt";
    std::string_view c_contents = "The contents of b/c.txt";
    {
        auto writer = createArchiveWriter(getPathToArchive());
        {
            auto out = writer->writeFile("a.txt");
            writeString(a_contents, *out);
        }
        {
            auto out = writer->writeFile("b/c.txt");
            writeString(c_contents, *out);
        }
    }

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    ASSERT_TRUE(reader->fileExists("a.txt"));
    ASSERT_TRUE(reader->fileExists("b/c.txt"));

    EXPECT_EQ(reader->getFileInfo("a.txt").uncompressed_size, a_contents.size());
    EXPECT_EQ(reader->getFileInfo("b/c.txt").uncompressed_size, c_contents.size());

    {
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }

    {
        auto in = reader->readFile("b/c.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, c_contents);
    }

    {
        /// Read a.txt again.
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }

    {
        /// Use an enumerator.
        auto enumerator = reader->firstFile();
        ASSERT_NE(enumerator, nullptr);
        EXPECT_EQ(enumerator->getFileName(), "a.txt");
        EXPECT_EQ(enumerator->getFileInfo().uncompressed_size, a_contents.size());
        EXPECT_TRUE(enumerator->nextFile());
        EXPECT_EQ(enumerator->getFileName(), "b/c.txt");
        EXPECT_EQ(enumerator->getFileInfo().uncompressed_size, c_contents.size());
        EXPECT_FALSE(enumerator->nextFile());
    }

    {
        /// Use converting an enumerator to a reading buffer and vice versa.
        auto enumerator = reader->firstFile();
        ASSERT_NE(enumerator, nullptr);
        EXPECT_EQ(enumerator->getFileName(), "a.txt");
        auto in = reader->readFile(std::move(enumerator));
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
        enumerator = reader->nextFile(std::move(in));
        ASSERT_NE(enumerator, nullptr);
        EXPECT_EQ(enumerator->getFileName(), "b/c.txt");
        in = reader->readFile(std::move(enumerator));
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, c_contents);
        enumerator = reader->nextFile(std::move(in));
        EXPECT_EQ(enumerator, nullptr);
    }
}


TEST_P(ArchiveReaderAndWriterTest, InMemory)
{
    String archive_in_memory;

    /// Make an archive.
    std::string_view a_contents = "The contents of a.txt";
    std::string_view b_contents = "The contents of b.txt";
    {
        auto writer = createArchiveWriter(getPathToArchive(), std::make_unique<WriteBufferFromString>(archive_in_memory));
        {
            auto out = writer->writeFile("a.txt");
            writeString(a_contents, *out);
        }
        {
            auto out = writer->writeFile("b.txt");
            writeString(b_contents, *out);
        }
    }

    /// The created archive is really in memory.
    ASSERT_FALSE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto read_archive_func = [&]() -> std::unique_ptr<SeekableReadBuffer> { return std::make_unique<ReadBufferFromString>(archive_in_memory); };
    auto reader = createArchiveReader(getPathToArchive(), read_archive_func, archive_in_memory.size());

    ASSERT_TRUE(reader->fileExists("a.txt"));
    ASSERT_TRUE(reader->fileExists("b.txt"));

    EXPECT_EQ(reader->getFileInfo("a.txt").uncompressed_size, a_contents.size());
    EXPECT_EQ(reader->getFileInfo("b.txt").uncompressed_size, b_contents.size());

    {
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }

    {
        auto in = reader->readFile("b.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, b_contents);
    }

    {
        /// Read a.txt again.
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }
}


TEST_P(ArchiveReaderAndWriterTest, Password)
{
    /// Make an archive.
    std::string_view contents = "The contents of a.txt";
    {
        auto writer = createArchiveWriter(getPathToArchive());
        writer->setPassword("Qwe123");
        {
            auto out = writer->writeFile("a.txt");
            writeString(contents, *out);
        }
    }

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    /// Try to read without a password.
    expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Password is required",
                    [&]{ reader->readFile("a.txt"); });

    {
        /// Try to read with a wrong password.
        reader->setPassword("123Qwe");
        expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Wrong password",
                        [&]{ reader->readFile("a.txt"); });
    }

    {
        /// Reading with the right password is successful.
        reader->setPassword("Qwe123");
        auto in = reader->readFile("a.txt");
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }
}


TEST_P(ArchiveReaderAndWriterTest, ArchiveNotExist)
{
    expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open",
                    [&]{ createArchiveReader(getPathToArchive()); });
}


#if USE_MINIZIP

namespace
{
    const char * supported_archive_file_exts[] =
    {
        ".zip",
    };
}

INSTANTIATE_TEST_SUITE_P(All, ArchiveReaderAndWriterTest, ::testing::ValuesIn(supported_archive_file_exts));

#endif
