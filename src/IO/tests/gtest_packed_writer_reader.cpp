#include <gtest/gtest.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteHelpers.h>
#include <Disks/DiskLocal.h>

using namespace DB;

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

    writer.finalize([&](String serialised_data, const auto & settings, bool need_sync)
    {
        auto buf = disk->writeFile(data_filename, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
        buf->write(serialised_data.data(), serialised_data.size());
        buf->finalize();
        if (need_sync)
            buf->sync();
    }, {}, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

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

    auto write_callback = [&](String serialised_data, const auto & settings, bool need_sync)
    {
        auto buf = disk->writeFile(data_filename, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
        buf->write(serialised_data.data(), serialised_data.size());
        buf->finalize();
        if (need_sync)
            buf->sync();
    };

    PackedFilesWriter writer1;

    {
        auto out1 = writer1.writeFile("file1");
        writeString("123", *out1);
        out1->finalize();

        auto out2 = writer1.writeFile("file2");
        writeString("456", *out2);
        out2->finalize();
    }

    auto old_index = writer1.finalize(write_callback, {}, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

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
    auto new_index = writer2.finalize(write_callback, {}, PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);

    ASSERT_EQ(old_index.size(), 1);
    ASSERT_FALSE(old_index.contains("file1"));
    ASSERT_TRUE(old_index.contains("file2"));

    PackedFilesReader reader(disk, data_filename, getReadSettings());
    auto in = reader.readFile(disk, data_filename, "file3", ReadSettings{}, {});
    assertString("101", *in);
}
