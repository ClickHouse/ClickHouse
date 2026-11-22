#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>

/// The append-only `Log`/`TinyLog`/`StripeLog` engines write each insert with the current default codec
/// and append it to the same compressed stream. A server upgrade that changes the default codec
/// (for example `LZ4` -> `ZSTD`) therefore produces a single `.bin`/`index` file whose blocks use
/// different codecs. Each compressed block is self-describing (its codec method byte is in the header),
/// so such a stream is valid and must read back correctly when `allow_different_codecs = true` - which is
/// exactly what those readers now pass. These tests pin that contract on the precise buffer types the
/// Log family uses: `CompressedReadBuffer` (Log/TinyLog) and `CompressedReadBufferFromFile` (StripeLog).

namespace DB::ErrorCodes
{
extern const int CANNOT_DECOMPRESS;
}

namespace
{
using namespace DB;

/// Two compressed blocks back to back: `first` under `LZ4` (method byte 0x82), `second` under `ZSTD(3)` (0x90).
/// This mirrors a file written before an upgrade (LZ4) and appended to after the upgrade (ZSTD).
std::string makeLz4ThenZstdStream(const std::string & first, const std::string & second)
{
    auto lz4_codec = CompressionCodecFactory::instance().get("LZ4", {});
    auto zstd_codec = CompressionCodecFactory::instance().get("ZSTD", 3);

    WriteBufferFromOwnString out;
    {
        CompressedWriteBuffer block(out, lz4_codec);
        block.write(first.data(), first.size());
        block.finalize();
    }
    {
        CompressedWriteBuffer block(out, zstd_codec);
        block.write(second.data(), second.size());
        block.finalize();
    }
    return out.str();
}

TEST(MixedCodecAppend, CompressedReadBufferThrowsWithoutTolerance)
{
    /// Sanity check: the default `allow_different_codecs = false` must reject the mixed stream.
    /// This is the failure an upgraded Log/TinyLog table would hit without the fix.
    const std::string stream = makeLz4ThenZstdStream("written under LZ4, ", "appended under ZSTD(3).");
    ReadBufferFromString source(stream);
    CompressedReadBuffer decompressor(source, /* allow_different_codecs = */ false);

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

TEST(MixedCodecAppend, CompressedReadBufferReadsMixedStream)
{
    /// The Log/TinyLog read path: one `CompressedReadBuffer` walks the whole data file across the
    /// append boundary. With tolerance on, both the LZ4 and the ZSTD block must decode.
    const std::string first = "written under LZ4, ";
    const std::string second = "appended under ZSTD(3).";
    const std::string stream = makeLz4ThenZstdStream(first, second);

    ReadBufferFromString source(stream);
    CompressedReadBuffer decompressor(source, /* allow_different_codecs = */ true);

    std::string decompressed;
    readStringUntilEOF(decompressed, decompressor);
    EXPECT_EQ(decompressed, first + second);
}

TEST(MixedCodecAppend, CompressedReadBufferFromFileReadsMixedStream)
{
    /// The StripeLog read path: `CompressedReadBufferFromFile` over `data.bin`/`index` files that were
    /// appended across an upgrade. With tolerance on, the mixed-codec file must decode.
    const std::string first = "written under LZ4, ";
    const std::string second = "appended under ZSTD(3).";
    const std::string stream = makeLz4ThenZstdStream(first, second);

    auto tmp_file = createTemporaryFile("/tmp/");
    {
        WriteBufferFromFile out(tmp_file->path());
        out.write(stream.data(), stream.size());
        out.finalize();
    }

    CompressedReadBufferFromFile in(std::make_unique<ReadBufferFromFile>(tmp_file->path()), /* allow_different_codecs = */ true);

    std::string decompressed;
    readStringUntilEOF(decompressed, in);
    EXPECT_EQ(decompressed, first + second);
}

}
