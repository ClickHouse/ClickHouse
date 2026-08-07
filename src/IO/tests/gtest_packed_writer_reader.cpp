#include <gtest/gtest.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteHelpers.h>
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
