#include <gtest/gtest.h>
#include <IO/PackedFilesWriter.h>
#include <IO/PackedFilesReader.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Disks/DiskLocal.h>

namespace DB::ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

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

/// A member view seeks inside the archive, and a direct-IO archive buffer reports a seek position floored to
/// its alignment (ReadBufferFromFileDescriptor::seek) even though it lands exactly where asked -- the view
/// must trust the position it requested. `alignment` reproduces that flooring without needing O_DIRECT, and a
/// buffer smaller than the alignment keeps the targets outside the buffered window, where seek really floors.
namespace
{

constexpr size_t ALIGNMENT = DEFAULT_AIO_FILE_BLOCK_SIZE;
constexpr size_t VIEW_SEEK_BIG_SIZE = 5 * ALIGNMENT;
constexpr auto VIEW_SEEK_ARCHIVE = "data_aligned_seek.packed";

/// Writes an archive holding "big" (several alignment blocks) followed by "tail", whose bytes a read running
/// past the end of "big" would return. Returns the members' string contents.
std::pair<String, String> writeViewSeekArchive(const DiskPtr & disk)
{
    const String big(VIEW_SEEK_BIG_SIZE, 'b');
    const String tail(16, 't');

    PackedFilesWriter writer;
    {
        auto out_big = writer.writeFile("big");
        writeString(big, *out_big);
        out_big->finalize();

        auto out_tail = writer.writeFile("tail");
        writeString(tail, *out_tail);
        out_tail->finalize();
    }
    writeArchive(disk, VIEW_SEEK_ARCHIVE, writer);

    return {big, tail};
}

std::unique_ptr<ReadBufferFromFile> openAligned()
{
    return std::make_unique<ReadBufferFromFile>(
        "tmp/" + String(VIEW_SEEK_ARCHIVE), /*buf_size=*/1024, -1, nullptr, ALIGNMENT);
}

}

/// Land 100 bytes past a block boundary: the floored position is 100 bytes short but still inside the member,
/// so a view that trusts it under-tracks its offset and reads past the member into "tail".
TEST(PackedFilesReader, ViewSeekPastBlockBoundary)
{
    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");
    const auto [big, tail] = writeViewSeekArchive(disk);

    PackedFilesReader reader(disk, VIEW_SEEK_ARCHIVE, getReadSettings());
    const auto big_at = reader.getFileOffsetAndSize("big");

    /// The index puts "big" mid-block, which is what makes a floored position differ from the requested one.
    const size_t offset_in_block = big_at.offset % ALIGNMENT;
    ASSERT_GT(offset_in_block, 0);

    const size_t seek_to = (ALIGNMENT - offset_in_block) + 2 * ALIGNMENT + 100;
    auto view = PackedFilesReader::viewMember(openAligned(), "big", big_at.offset, big_at.size);

    ASSERT_EQ(view->seek(seek_to, SEEK_SET), seek_to);
    ASSERT_EQ(view->getPosition(), seek_to);

    String rest;
    readStringUntilEOF(rest, *view);
    ASSERT_EQ(rest.size(), VIEW_SEEK_BIG_SIZE - seek_to);
    ASSERT_EQ(rest, big.substr(seek_to));
}

/// Land in the same block the member starts in: the floored position is below the member's own start, which a
/// view that trusts it rejects as out of range.
TEST(PackedFilesReader, ViewSeekWithinStartBlock)
{
    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");
    const auto [big, tail] = writeViewSeekArchive(disk);

    PackedFilesReader reader(disk, VIEW_SEEK_ARCHIVE, getReadSettings());
    const auto big_at = reader.getFileOffsetAndSize("big");

    const size_t offset_in_block = big_at.offset % ALIGNMENT;
    ASSERT_GT(offset_in_block, 0);
    /// The target must clear the buffered window, or seek returns the exact position and proves nothing.
    ASSERT_LT(offset_in_block, ALIGNMENT - 100 - 1024);

    const size_t seek_to = ALIGNMENT - 100 - offset_in_block;
    auto view = PackedFilesReader::viewMember(openAligned(), "big", big_at.offset, big_at.size);

    ASSERT_EQ(view->seek(seek_to, SEEK_SET), seek_to);
    ASSERT_EQ(view->getPosition(), seek_to);

    String rest;
    readStringUntilEOF(rest, *view);
    ASSERT_EQ(rest, big.substr(seek_to));
}

/// A read-until narrows the view: seeking past it must be refused, not accepted and then silently clamped by
/// the impl (ReadBufferFromEncryptedFile and AsynchronousBoundedReadBuffer clamp instead of failing, which
/// would leave the view reporting a position its impl never took).
TEST(PackedFilesReader, ViewSeekPastReadUntil)
{
    fs::create_directory("tmp/");
    DiskPtr disk = std::make_shared<DiskLocal>("local_disk", "tmp/");
    const auto [big, tail] = writeViewSeekArchive(disk);

    PackedFilesReader reader(disk, VIEW_SEEK_ARCHIVE, getReadSettings());
    const auto big_at = reader.getFileOffsetAndSize("big");

    auto view = PackedFilesReader::viewMember(openAligned(), "big", big_at.offset, big_at.size);
    view->setReadUntilPosition(2 * ALIGNMENT);

    /// Still inside the read-until: allowed, and the bytes are the member's own.
    ASSERT_EQ(view->seek(ALIGNMENT + 100, SEEK_SET), ALIGNMENT + 100);
    ASSERT_EQ(view->getPosition(), ALIGNMENT + 100);

    try
    {
        view->seek(2 * ALIGNMENT + 1, SEEK_SET);
        FAIL() << "seek past the read-until position was accepted";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }

    /// The whole member is reachable again once the read-until is lifted.
    view->setReadUntilEnd();
    ASSERT_EQ(view->seek(VIEW_SEEK_BIG_SIZE - 10, SEEK_SET), VIEW_SEEK_BIG_SIZE - 10);
}
