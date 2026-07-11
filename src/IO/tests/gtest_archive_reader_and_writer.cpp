#include <gtest/gtest.h>
#include "config.h"

#include <filesystem>

#include <IO/Archives/ArchiveUtils.h>
#include <IO/Archives/IArchiveReader.h>
#include <IO/Archives/IArchiveWriter.h>
#include <IO/Archives/createArchiveReader.h>
#include <IO/Archives/createArchiveWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/TemporaryFile.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <Common/thread_local_rng.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_PACK_ARCHIVE;
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace fs = std::filesystem;
using namespace DB;

enum class ArchiveType : uint8_t
{
    Tar,
    SevenZip
};

template <ArchiveType archive_type>
bool createArchiveWithFiles(const std::string & archivename, const std::map<std::string, std::string> & files)
{
    struct archive * a;
    struct archive_entry * entry;

    a = archive_write_new();

    if constexpr (archive_type == ArchiveType::Tar)
        archive_write_set_format_pax_restricted(a);
    else if constexpr (archive_type == ArchiveType::SevenZip)
        archive_write_set_format_7zip(a);
    else
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Invalid archive type requested: {}", static_cast<size_t>(archive_type));

    archive_write_open_filename(a, archivename.c_str());

    for (const auto & [filename, content] : files)
    {
        entry = archive_entry_new();
        archive_entry_set_pathname(entry, filename.c_str());
        archive_entry_set_size(entry, content.size());
        archive_entry_set_mode(entry, S_IFREG | 0644); // regular file with rw-r--r-- permissions
        archive_entry_set_mtime(entry, time(nullptr), 0);
        archive_write_header(a, entry);
        archive_write_data(a, content.c_str(), content.size());
        archive_entry_free(entry);
    }

    archive_write_close(a);
    archive_write_free(a);

    return true;
}

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
        auto writer = createArchiveWriter(getPathToArchive());
        writer->finalize();
    }

    /// The created archive can be found in the local filesystem.
    ASSERT_TRUE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    EXPECT_FALSE(reader->fileExists("nofile.txt"));

    expectException(
        ErrorCodes::CANNOT_UNPACK_ARCHIVE, "File 'nofile.txt' was not found in archive", [&] { reader->getFileInfo("nofile.txt"); });

    expectException(
        ErrorCodes::CANNOT_UNPACK_ARCHIVE,
        "File 'nofile.txt' was not found in archive",
        [&] { reader->readFile("nofile.txt", /*throw_on_not_found=*/true); });

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
            out->finalize();
        }
        writer->finalize();
    }

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    ASSERT_TRUE(reader->fileExists("a.txt"));

    auto file_info = reader->getFileInfo("a.txt");
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    EXPECT_GT(file_info.compressed_size, 0);

    {
        auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
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
        expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "No current file", [&] { enumerator->getFileName(); });

        expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "No current file", [&] { reader->readFile(std::move(enumerator)); });
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
            out->finalize();
        }
        {
            auto out = writer->writeFile("b/c.txt");
            writeString(c_contents, *out);
            out->finalize();
        }
        writer->finalize();
    }

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    ASSERT_TRUE(reader->fileExists("a.txt"));
    ASSERT_TRUE(reader->fileExists("b/c.txt"));

    // Get all files
    auto files = reader->getAllFiles();
    EXPECT_EQ(files.size(), 2);

    EXPECT_EQ(reader->getFileInfo("a.txt").uncompressed_size, a_contents.size());
    EXPECT_EQ(reader->getFileInfo("b/c.txt").uncompressed_size, c_contents.size());

    {
        auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }

    {
        auto in = reader->readFile("b/c.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, c_contents);
    }

    {
        /// Read a.txt again.
        auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
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

    // Get all files one last time
    files = reader->getAllFiles();
    EXPECT_EQ(files.size(), 2);
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
            out->finalize();
        }
        {
            auto out = writer->writeFile("b.txt");
            writeString(b_contents, *out);
            out->finalize();
        }
        writer->finalize();
    }

    /// The created archive is really in memory.
    ASSERT_FALSE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto read_archive_func
        = [&]() -> std::unique_ptr<SeekableReadBuffer> { return std::make_unique<ReadBufferFromString>(archive_in_memory); };
    auto reader = createArchiveReader(getPathToArchive(), read_archive_func, archive_in_memory.size());

    ASSERT_TRUE(reader->fileExists("a.txt"));
    ASSERT_TRUE(reader->fileExists("b.txt"));

    EXPECT_EQ(reader->getFileInfo("a.txt").uncompressed_size, a_contents.size());
    EXPECT_EQ(reader->getFileInfo("b.txt").uncompressed_size, b_contents.size());

    {
        auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }

    {
        auto in = reader->readFile("b.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, b_contents);
    }

    {
        /// Read a.txt again.
        auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, a_contents);
    }
}


TEST_P(ArchiveReaderAndWriterTest, ManyFilesInMemory)
{
    String archive_in_memory;
    int files = 1000;
    size_t times = 1;
    /// Make an archive.
    {
        auto writer = createArchiveWriter(getPathToArchive(), std::make_unique<WriteBufferFromString>(archive_in_memory));
        {
            for (int i = 0; i < files; i++)
            {
                auto filename = fmt::format("{}.txt", i);
                auto contents = fmt::format("The contents of {}.txt", i);
                auto out = writer->writeFile(filename, times * contents.size());
                for (int j = 0; j < times; j++)
                    writeString(contents, *out);
                out->finalize();
            }
        }
        writer->finalize();
    }

    /// The created archive is really in memory.
    ASSERT_FALSE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto read_archive_func
        = [&]() -> std::unique_ptr<SeekableReadBuffer> { return std::make_unique<ReadBufferFromString>(archive_in_memory); };
    auto reader = createArchiveReader(getPathToArchive(), read_archive_func, archive_in_memory.size());

    for (int i = 0; i < files; i++)
    {
        auto filename = fmt::format("{}.txt", i);
        auto contents = fmt::format("The contents of {}.txt", i);
        ASSERT_TRUE(reader->fileExists(filename));
        EXPECT_EQ(reader->getFileInfo(filename).uncompressed_size, times * contents.size());

        {
            auto in = reader->readFile(filename, /*throw_on_not_found=*/true);
            for (int j = 0; j < times; j++)
                ASSERT_TRUE(checkString(String(contents), *in));
        }
    }
}

TEST_P(ArchiveReaderAndWriterTest, Password)
{
    auto writer = createArchiveWriter(getPathToArchive());
    //don't support passwords for tar archives
    if (getPathToArchive().ends_with(".tar") || getPathToArchive().ends_with(".tar.gz") || getPathToArchive().ends_with(".tar.bz2")
        || getPathToArchive().ends_with(".tar.lzma") || getPathToArchive().ends_with(".tar.zst") || getPathToArchive().ends_with(".tar.xz"))
    {
        expectException(
            ErrorCodes::NOT_IMPLEMENTED,
            "Setting a password is not currently supported for libarchive",
            [&] { writer->setPassword("a.txt"); });
        writer->finalize();
    }
    else
    {
        /// Make an archive.
        std::string_view contents = "The contents of a.txt";
        {
            writer->setPassword("Qwe123");
            {
                auto out = writer->writeFile("a.txt");
                writeString(contents, *out);
                out->finalize();
            }
            writer->finalize();
        }

        /// Read the archive.
        auto reader = createArchiveReader(getPathToArchive());

        /// Try to read without a password.
        expectException(
            ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Password is required", [&] { reader->readFile("a.txt", /*throw_on_not_found=*/true); });

        {
            /// Try to read with a wrong password.
            reader->setPassword("123Qwe");
            expectException(
                ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Wrong password", [&] { reader->readFile("a.txt", /*throw_on_not_found=*/true); });
        }

        {
            /// Reading with the right password is successful.
            reader->setPassword("Qwe123");
            auto in = reader->readFile("a.txt", /*throw_on_not_found=*/true);
            String str;
            readStringUntilEOF(str, *in);
            EXPECT_EQ(str, contents);
        }
    }
}


TEST_P(ArchiveReaderAndWriterTest, ArchiveNotExist)
{
    expectException(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open", [&] { createArchiveReader(getPathToArchive()); });
}


TEST_P(ArchiveReaderAndWriterTest, ManyFilesOnDisk)
{
    int files = 1000;
    size_t times = 1;
    /// Make an archive.
    {
        auto writer = createArchiveWriter(getPathToArchive());
        {
            for (int i = 0; i < files; i++)
            {
                auto filename = fmt::format("{}.txt", i);
                auto contents = fmt::format("The contents of {}.txt", i);
                auto out = writer->writeFile(filename, times * contents.size());
                for (int j = 0; j < times; j++)
                    writeString(contents, *out);
                out->finalize();
            }
        }
        writer->finalize();
    }

    /// The created archive is really in memory.
    ASSERT_TRUE(fs::exists(getPathToArchive()));

    /// Read the archive.
    auto reader = createArchiveReader(getPathToArchive());

    for (int i = 0; i < files; i++)
    {
        auto filename = fmt::format("{}.txt", i);
        auto contents = fmt::format("The contents of {}.txt", i);
        ASSERT_TRUE(reader->fileExists(filename));
        EXPECT_EQ(reader->getFileInfo(filename).uncompressed_size, times * contents.size());

        {
            auto in = reader->readFile(filename, /*throw_on_not_found=*/true);
            for (int j = 0; j < times; j++)
                ASSERT_TRUE(checkString(String(contents), *in));
        }
    }
}

TEST(TarArchiveReaderTest, FileExists)
{
    String archive_path = "archive.tar";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::Tar>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    EXPECT_EQ(reader->fileExists(filename), true);
    fs::remove(archive_path);
}

TEST(TarArchiveReaderTest, ReadFile)
{
    String archive_path = "archive.tar";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::Tar>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    auto in = reader->readFile(filename, /*throw_on_not_found=*/true);
    String str;
    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents);
    fs::remove(archive_path);
}

TEST(TarArchiveReaderTest, ReadTwoFiles)
{
    String archive_path = "archive.tar";
    String file1 = "file1.txt";
    String contents1 = "test1";
    String file2 = "file2.txt";
    String contents2 = "test2";
    bool created = createArchiveWithFiles<ArchiveType::Tar>(archive_path, {{file1, contents1}, {file2, contents2}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    EXPECT_EQ(reader->fileExists(file1), true);
    EXPECT_EQ(reader->fileExists(file2), true);
    auto in = reader->readFile(file1, /*throw_on_not_found=*/true);
    String str;
    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents1);
    in = reader->readFile(file2, /*throw_on_not_found=*/true);

    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents2);
    fs::remove(archive_path);
}


TEST(TarArchiveReaderTest, CheckFileInfo)
{
    String archive_path = "archive.tar";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::Tar>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    auto info = reader->getFileInfo(filename);
    EXPECT_EQ(info.uncompressed_size, contents.size());
    EXPECT_GT(info.compressed_size, 0);
    fs::remove(archive_path);
}

TEST(TarArchiveReaderAndWriterTest, BufferSizeLimitExceededUnknownContentsSize)
{
    thread_local_rng.seed(42);

    String archive_path = "archive.tar";
    String file_path = "a.txt";
    std::string contents = getRandomASCIIString(1025);
    {
        auto writer = createArchiveWriter(
            archive_path,
            /*archive_write_buffer_*/ nullptr,
            /*buf_size_*/ 1024,
            /*adaptive_buffer_max_size_*/ 1024);
        {
            auto out = writer->writeFile(file_path);
            EXPECT_THROW(writeString(contents, *out), DB::Exception);
        }
        writer->finalize();
    }
}

TEST(TarArchiveReaderAndWriterTest, SmallBufferContentsSizeSet)
{
    thread_local_rng.seed(42);

    String archive_path = "archive.tar";
    String file_path = "a.txt";
    std::string contents = getRandomASCIIString(1025);
    {
        auto writer = createArchiveWriter(
            archive_path,
            /*archive_write_buffer_*/ nullptr,
            /*buf_size_*/ 1024,
            /*adaptive_buffer_max_size_*/ 1024);
        {
            auto out = writer->writeFile(file_path, contents.size());
            writeString(contents, *out);
            out->finalize();
        }
        writer->finalize();
    }
    auto reader = createArchiveReader(archive_path);

    ASSERT_TRUE(reader->fileExists(file_path));

    auto file_info = reader->getFileInfo(file_path);
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    {
        auto in = reader->readFile(file_path, /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }
}

TEST(TarArchiveReaderAndWriterTest, AdaptiveBuffer)
{
    thread_local_rng.seed(42);

    String archive_path = "archive.tar";
    String file_path = "a.txt";
    std::string contents = getRandomASCIIString(2049);
    {
        auto writer = createArchiveWriter(
            archive_path,
            /*archive_write_buffer_*/ nullptr,
            /*buf_size_*/ 1024,
            /*adaptive_buffer_max_size_*/ 4096);
        {
            auto out = writer->writeFile(file_path);
            writeString(contents, *out);
            out->finalize();
        }
        writer->finalize();
    }
    auto reader = createArchiveReader(archive_path);

    ASSERT_TRUE(reader->fileExists(file_path));

    auto file_info = reader->getFileInfo(file_path);
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    {
        auto in = reader->readFile(file_path, /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }
}

TEST(TarArchiveReaderAndWriterTest, AdaptiveBufferPowerOfTwoSize)
{
    thread_local_rng.seed(42);

    String archive_path = "archive.tar";
    String file_path = "a.txt";
    std::string contents = getRandomASCIIString(2048);
    {
        auto writer = createArchiveWriter(
            archive_path,
            /*archive_write_buffer_*/ nullptr,
            /*buf_size_*/ 1024,
            /*adaptive_buffer_max_size_*/ 4096);
        {
            auto out = writer->writeFile(file_path);
            writeString(contents, *out);
            out->finalize();
        }
        writer->finalize();
    }
    auto reader = createArchiveReader(archive_path);

    ASSERT_TRUE(reader->fileExists(file_path));

    auto file_info = reader->getFileInfo(file_path);
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    {
        auto in = reader->readFile(file_path, /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }
}

TEST(TarArchiveReaderAndWriterTest, AdaptiveBufferMaxCapacity)
{
    thread_local_rng.seed(42);

    String archive_path = "archive.tar";
    String file_path = "a.txt";
    std::string contents = getRandomASCIIString(4096);
    {
        auto writer = createArchiveWriter(
            archive_path,
            /*archive_write_buffer_*/ nullptr,
            /*buf_size_*/ 1024,
            /*adaptive_buffer_max_size_*/ 4096);
        {
            auto out = writer->writeFile(file_path);
            writeString(contents, *out);
            out->finalize();
        }
        writer->finalize();
    }
    auto reader = createArchiveReader(archive_path);

    ASSERT_TRUE(reader->fileExists(file_path));

    auto file_info = reader->getFileInfo(file_path);
    EXPECT_EQ(file_info.uncompressed_size, contents.size());
    {
        auto in = reader->readFile(file_path, /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, contents);
    }
}

TEST(TarArchiveReaderAndWriterTest, EmptyFileWithKnownSize)
{
    /// This test exercises the code path where writeFile(filename, size) is called
    /// with size=0 and no data is written. Previously, expected_size was uninitialized
    /// in this case, causing a MSan use-of-uninitialized-value in closeFile.
    String archive_path = "archive.tar";
    {
        auto writer = createArchiveWriter(archive_path);
        {
            auto out = writer->writeFile("empty.txt", 0);
            out->finalize();
        }
        {
            auto out = writer->writeFile("non_empty.txt", 4);
            writeString("test", *out);
            out->finalize();
        }
        writer->finalize();
    }
    /// The empty file won't appear in the archive because writeEntry is only called
    /// when data is actually written. The important thing is that finalizing the empty
    /// file's buffer does not trigger any undefined behavior (MSan).
    auto reader = createArchiveReader(archive_path);
    ASSERT_FALSE(reader->fileExists("empty.txt"));
    ASSERT_TRUE(reader->fileExists("non_empty.txt"));
    {
        auto in = reader->readFile("non_empty.txt", /*throw_on_not_found=*/true);
        String str;
        readStringUntilEOF(str, *in);
        EXPECT_EQ(str, "test");
    }
    fs::remove(archive_path);
}

TEST(SevenZipArchiveReaderTest, FileExists)
{
    String archive_path = "archive.7z";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::SevenZip>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    EXPECT_EQ(reader->fileExists(filename), true);
    fs::remove(archive_path);
}

TEST(SevenZipArchiveReaderTest, ReadFile)
{
    String archive_path = "archive.7z";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::SevenZip>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    auto in = reader->readFile(filename, /*throw_on_not_found=*/true);
    String str;
    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents);
    fs::remove(archive_path);
}

TEST(SevenZipArchiveReaderTest, CheckFileInfo)
{
    String archive_path = "archive.7z";
    String filename = "file.txt";
    String contents = "test";
    bool created = createArchiveWithFiles<ArchiveType::SevenZip>(archive_path, {{filename, contents}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    auto info = reader->getFileInfo(filename);
    EXPECT_EQ(info.uncompressed_size, contents.size());
    EXPECT_GT(info.compressed_size, 0);
    fs::remove(archive_path);
}

TEST(SevenZipArchiveReaderTest, ReadTwoFiles)
{
    String archive_path = "archive.7z";
    String file1 = "file1.txt";
    String contents1 = "test1";
    String file2 = "file2.txt";
    String contents2 = "test2";
    bool created = createArchiveWithFiles<ArchiveType::SevenZip>(archive_path, {{file1, contents1}, {file2, contents2}});
    EXPECT_EQ(created, true);
    auto reader = createArchiveReader(archive_path);
    EXPECT_EQ(reader->fileExists(file1), true);
    EXPECT_EQ(reader->fileExists(file2), true);
    auto in = reader->readFile(file1, /*throw_on_not_found=*/true);
    String str;
    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents1);
    in = reader->readFile(file2, /*throw_on_not_found=*/true);

    readStringUntilEOF(str, *in);
    EXPECT_EQ(str, contents2);
    fs::remove(archive_path);
}


/// A WriteBuffer that throws after a specified number of bytes, simulating a disk-full condition.
class ThrowAfterNBytesWriteBuffer : public WriteBufferFromFileBase
{
public:
    explicit ThrowAfterNBytesWriteBuffer(size_t throw_after_bytes_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , throw_after_bytes(throw_after_bytes_)
    {
    }

    void sync() override { }
    std::string getFileName() const override { return "ThrowAfterNBytesWriteBuffer"; }

private:
    void nextImpl() override
    {
        size_t to_write = offset();
        if (bytes_written + to_write > throw_after_bytes)
            throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Simulated disk full error after {} bytes", bytes_written);
        bytes_written += to_write;
    }

    size_t throw_after_bytes;
    size_t bytes_written = 0;
};


/// Test that write errors in the underlying buffer during archive creation produce
/// a proper exception instead of std::terminate (which happens if C++ exceptions
/// propagate through C library code like minizip or libarchive).
TEST_P(ArchiveReaderAndWriterTest, WriteErrorProducesException)
{
    /// Allow writing some data so the archive header gets created, then fail.
    auto failing_buffer = std::make_unique<ThrowAfterNBytesWriteBuffer>(1024);
    auto writer = createArchiveWriter(getPathToArchive(), std::move(failing_buffer));

    auto out = writer->writeFile("a.txt");
    /// Write enough random (incompressible) data to trigger the underlying buffer flush failure.
    /// Using random data ensures that compressed formats (bz2, lzma, zst, xz) also exceed
    /// the byte threshold, since repetitive data compresses to nearly nothing.
    String large_content = getRandomASCIIString(1024 * 1024);
    EXPECT_THROW(
        {
            writeString(large_content, *out);
            out->finalize();
            writer->finalize();
        },
        Exception);

    /// Clean up after the expected exception: the writer was not finalized,
    /// so we must cancel it to avoid the chassert in the destructor.
    out.reset();
    writer->cancel();
}


namespace
{
const char * supported_archive_file_exts[] = {
#if USE_MINIZIP
    ".zip",
#endif
#if USE_LIBARCHIVE
    ".tar",
    ".tar.gz",
    ".tar.bz2",
    ".tar.lzma",
    ".tar.zst",
    ".tar.xz",
#endif
};
}

INSTANTIATE_TEST_SUITE_P(All, ArchiveReaderAndWriterTest, ::testing::ValuesIn(supported_archive_file_exts));
