#include <gtest/gtest.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/SpillableMemoryWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Disks/DiskLocal.h>

using namespace DB;

/// Writes the archive of @writer into @file_name on @disk and returns its index.
static PackedFilesIO::Index writeArchive(const DiskPtr & disk, const String & file_name, PackedFilesWriter & writer)
{
    auto buf = disk->writeFile(file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, writer.getWriteSettings());
    auto [index, need_sync] = writer.finalize(*buf, {}, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

    buf->finalize();
    if (need_sync)
        buf->sync();

    return index;
}

TEST(PackedFilesWriter, Basics)
{
    static constexpr auto data_filename = "data.packed";

    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");

    PackedFilesWriter writer;

    {
        auto out1 = writer.writeFile("file1");
        writeString("123", *out1);

        auto out2 = writer.writeFile("file2");
        auto out3 = writer.writeFile("file3");

        writeString("45", *out2);
        writeString("ab", *out1);
        writeString("qwert", *out3);
        writeString("as", *out3);
        writeString("67890", *out1);
        writeString("123", *out2);

        out3->finalize();
        out2->finalize();
        out1->finalize();
    }

    writeArchive(disk, data_filename, writer);

    PackedFilesReader reader(disk, data_filename, getReadSettings());

    auto check_file = [&](const String & name, const String & content)
    {
        auto in = reader.readFile(disk, data_filename, name, ReadSettings{}, {});
        assertString(content, *in);
        assertEOF(*in);

        ASSERT_TRUE(reader.exists(name));
        ASSERT_EQ(reader.getFileSize(name), content.size());
    };

    check_file("file1", "123ab67890");
    check_file("file2", "45123");
    check_file("file3", "qwertas");

    ASSERT_FALSE(reader.exists("file4"));

    {
        auto in = reader.readFile(disk, data_filename, "file1", ReadSettings{}, {});
        in->seek(1, SEEK_SET);
        assertChar('2', *in);
        in->seek(2, SEEK_CUR);
        assertChar('b', *in);
        assertChar('6', *in);
        in->seek(2, SEEK_SET);
        assertChar('3', *in);
        in->seek(8, SEEK_SET);
        assertString("90", *in);
        assertEOF(*in);
    }
}

TEST(PackedFilesWriter, Removes)
{
    static constexpr auto data_filename = "data.packed";

    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");

    PackedFilesWriter writer1;

    {
        auto out1 = writer1.writeFile("file1");
        writeString("123", *out1);
        out1->finalize();

        auto out2 = writer1.writeFile("file2");
        writeString("456", *out2);
        out2->finalize();
    }

    auto old_index = writeArchive(disk, data_filename, writer1);

    PackedFilesWriter writer2;

    {
        writer2.removeFile("file1");

        auto out3 = writer2.writeFile("file3");
        writeString("789", *out3);
        out3->finalize();

        writer2.removeFile("file3");

        auto out4 = writer2.writeFile("file3");
        writeString("101", *out4);
        out4->finalize();
    }

    writer2.applyMetadataChanges(old_index);
    auto new_index = writeArchive(disk, data_filename, writer2);

    ASSERT_EQ(old_index.size(), 1);
    ASSERT_FALSE(old_index.contains("file1"));
    ASSERT_TRUE(old_index.contains("file2"));

    PackedFilesReader reader(disk, data_filename, getReadSettings());
    auto in = reader.readFile(disk, data_filename, "file3", ReadSettings{}, {});
    assertString("101", *in);
}

/// `prepareFinalize` performs every check that can throw, so a caller can open the destination
/// file only after it succeeded and never truncate an existing archive on a failed finalization.
TEST(PackedFilesWriter, PrepareFinalizeDoesNotTouchDestination)
{
    static constexpr auto data_filename = "data_prepare.packed";

    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");

    PackedFilesWriter writer1;

    {
        auto out1 = writer1.writeFile("file1");
        writeString("123", *out1);
        out1->finalize();
    }

    auto old_index = writeArchive(disk, data_filename, writer1);
    const auto old_archive_size = disk->getFileSize(data_filename);

    PackedFilesWriter writer2;

    {
        auto out2 = writer2.writeFile("file2");
        writeString("456", *out2);
        out2->finalize();

        /// There is no such file, neither in the writer nor in the old index, so the change
        /// remains unapplied and finalization must fail.
        writer2.removeFile("file_that_does_not_exist");
    }

    writer2.applyMetadataChanges(old_index);

    ASSERT_THROW(
        writer2.prepareFinalize({}, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE),
        Exception);

    /// The old archive is intact: it was never opened for writing.
    ASSERT_EQ(disk->getFileSize(data_filename), old_archive_size);

    PackedFilesReader reader(disk, data_filename, getReadSettings());
    auto in = reader.readFile(disk, data_filename, "file1", ReadSettings{}, {});
    assertString("123", *in);
    assertEOF(*in);
}

TEST(PackedFilesWriter, SpillConfigExistsButNoSpill)
{
    static constexpr auto data_filename = "data_no_spill.packed";

    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");

    /// Spill config is set, but the written data is far below the capacity, so
    /// nothing is spilled and no spill file is created.
    int spill_write_creations = 0;
    auto spill_config = std::make_shared<PackedFilesWriter::SpillConfig>(
        4 * DBMS_DEFAULT_BUFFER_SIZE,
        [&, disk](const String & path) -> std::unique_ptr<WriteBufferFromFileBase>
        {
            ++spill_write_creations;
            return disk->writeFile(fs::path("spill_absent") / path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        },
        [disk](const String & path) -> std::unique_ptr<ReadBuffer>
        {
            return disk->readFile(fs::path("spill_absent") / path, ReadSettings{});
        },
        [disk]()
        {
            disk->createDirectories("spill_absent");
        },
        [disk]()
        {
            disk->removeRecursive("spill_absent");
        });

    PackedFilesWriter writer(std::move(spill_config));

    const String content1 = "short content";
    const String content2 = "another one";

    {
        auto out1 = writer.writeFile("file1");
        writeString(content1, *out1);
        out1->finalize();

        auto out2 = writer.writeFile("file2");
        writeString(content2, *out2);
        out2->finalize();
    }

    /// The capacity was never exceeded, so the spill write buffer was never created
    /// and the spill temp directory was not created either.
    ASSERT_EQ(0, spill_write_creations);
    ASSERT_FALSE(disk->existsDirectory("spill_absent"));

    auto index = writeArchive(disk, data_filename, writer);

    ASSERT_EQ(index.at("file1").size, content1.size());
    ASSERT_EQ(index.at("file2").size, content2.size());

    PackedFilesReader reader(disk, data_filename, getReadSettings());

    auto check_file = [&](const String & name, const String & content)
    {
        auto in = reader.readFile(disk, data_filename, name, ReadSettings{}, {});
        assertString(content, *in);
        assertEOF(*in);
    };

    check_file("file1", content1);
    check_file("file2", content2);
}

TEST(PackedFilesWriter, SpillsToDiskWhenMemoryLimitExceeded)
{
    static constexpr auto data_filename = "data_spill.packed";

    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");

    /// Tiny memory limit so that the total written data exceeds it and gets spilled.
    auto spill_config = std::make_shared<PackedFilesWriter::SpillConfig>(
        2048,
        [disk](const String & path) -> std::unique_ptr<WriteBufferFromFileBase>
        {
            return disk->writeFile(fs::path("spill_present") / path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        },
        [disk](const String & path) -> std::unique_ptr<ReadBuffer>
        {
            return disk->readFile(fs::path("spill_present") / path, ReadSettings{});
        },
        [disk]()
        {
            disk->createDirectories("spill_present");
        },
        [disk]()
        {
            disk->removeRecursive("spill_present");
        });

    PackedFilesWriter writer(std::move(spill_config));

    /// More than the first in-memory chunk (DBMS_DEFAULT_BUFFER_SIZE), so writing
    /// it triggers next() and the memory limit check, which spills the data.
    const String big_content(2 * DBMS_DEFAULT_BUFFER_SIZE, 'x');
    const String small_content = "small";

    {
        auto out1 = writer.writeFile("big_file");
        writeString(big_content, *out1);
        out1->finalize();

        auto out2 = writer.writeFile("small_file");
        writeString(small_content, *out2);
        out2->finalize();
    }

    /// More than the memory limit was written, so the data was spilled and the
    /// spill temp directory was created lazily.
    ASSERT_TRUE(disk->existsDirectory("spill_present"));

    /// The data was spilled to files in the temp directory.
    bool found_spill_file = false;
    for (auto it = disk->iterateDirectory("spill_present"); it->isValid(); it->next())
    {
        if (it->name() == "big_file" || it->name() == "small_file")
        {
            found_spill_file = true;
            break;
        }
    }
    ASSERT_TRUE(found_spill_file);

    auto index = writeArchive(disk, data_filename, writer);

    ASSERT_EQ(index.at("big_file").size, big_content.size());
    ASSERT_EQ(index.at("small_file").size, small_content.size());

    PackedFilesReader reader(disk, data_filename, getReadSettings());

    auto check_file = [&](const String & name, const String & content)
    {
        auto in = reader.readFile(disk, data_filename, name, ReadSettings{}, {});
        assertString(content, *in);
        assertEOF(*in);
    };

    check_file("big_file", big_content);
    check_file("small_file", small_content);
}
