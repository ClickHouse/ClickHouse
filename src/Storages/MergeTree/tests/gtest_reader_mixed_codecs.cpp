#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

/// The MergeTree reader uses `allow_different_codecs = true` by default, so one stream may hold blocks written with different codecs.
/// This drives `MergeTreeReaderStreamSingleColumnWholePart` over a`.bin` whose blocks use NONE then LZ4. The reader must work.

namespace DB::ErrorCodes
{
extern const int CANNOT_DECOMPRESS;
}

namespace
{
using namespace DB;
namespace fs = std::filesystem;

/// Two compressed blocks back to back: `first` under NONE (method byte 0x02), `second` under LZ4 (0x82).
std::string makeTwoBlockMixedCodecStream(const std::string & first, const std::string & second)
{
    auto none_codec = CompressionCodecFactory::instance().get("NONE", {});
    auto lz4_codec = CompressionCodecFactory::instance().get("LZ4", {});

    WriteBufferFromOwnString out;
    {
        CompressedWriteBuffer block(out, none_codec);
        block.write(first.data(), first.size());
        block.finalize();
    }
    {
        CompressedWriteBuffer block(out, lz4_codec);
        block.write(second.data(), second.size());
        block.finalize();
    }
    return out.str();
}

TEST(MixedCodecReaderTest, StreamGenuinelyMixesCodecs)
{
    /// Reading the crafted stream with the tolerance off must throw on the second block.
    const std::string stream = makeTwoBlockMixedCodecStream("first under NONE, ", "second under LZ4.");
    ReadBufferFromString source(stream);
    CompressedReadBuffer decompressor(source, /* allow_different_codecs */ false);

    std::string decompressed;
    try
    {
        readStringUntilEOF(decompressed, decompressor);
        FAIL() << "Expected CANNOT_DECOMPRESS, but read " << decompressed.size() << " bytes";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CANNOT_DECOMPRESS);
    }
}

struct MixedCodecReaderFixture : public ::testing::Test
{
    fs::path tmp_dir;
    DiskPtr disk;
    std::shared_ptr<SingleDiskVolume> volume;
    std::shared_ptr<DataPartStorageOnDiskFull> storage;

    void SetUp() override
    {
        tmp_dir = fs::temp_directory_path()
            / ("gtest_mixed_codec_reader_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_"
               + std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::remove_all(tmp_dir);
        fs::create_directories(tmp_dir / "part");
        disk = std::make_shared<DiskLocal>("test_disk", tmp_dir.string());
        volume = std::make_shared<SingleDiskVolume>("test_vol", disk);
        storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
    }

    void TearDown() override
    {
        storage.reset();
        volume.reset();
        disk.reset();
        fs::remove_all(tmp_dir);
    }
};

TEST_F(MixedCodecReaderFixture, ReaderReadsStreamWithDifferentCodecsPerBlock)
{
    const std::string first = "first block, stored verbatim under NONE, ";
    const std::string second = "second block, compressed under LZ4.";
    const std::string stream = makeTwoBlockMixedCodecStream(first, second);

    /// Write the crafted stream as the single column's `.bin` through the disk abstraction.
    {
        auto out = disk->writeFile("part/data.bin");
        out->write(stream.data(), stream.size());
        out->finalize();
    }

    auto settings = MergeTreeReaderSettings::createFromSettings();

    static constexpr size_t marks_count = 1;
    MergeTreeReaderStreamSingleColumnWholePart reader(
        storage,
        "data",
        ".bin",
        marks_count,
        MarkRanges{{0, marks_count}},
        settings,
        /*uncompressed_cache=*/nullptr,
        stream.size(),
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);

    std::string decompressed;
    readStringUntilEOF(decompressed, *reader.getDataBuffer());
    EXPECT_EQ(decompressed, first + second);
}

}
