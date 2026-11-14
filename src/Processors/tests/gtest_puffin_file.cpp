#include <gtest/gtest.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Processors/Formats/Impl/PuffinFile.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

namespace
{

String compress(const String & data, CompressionMethod method)
{
    WriteBufferFromOwnString out;
    auto compressed = wrapWriteBufferWithCompressionMethod(&out, method, /*level=*/ 3, /*zstd_window_log=*/ 0);
    writeString(data, *compressed);
    compressed->finalize();
    out.finalize();
    return out.str();
}

PuffinBlob makeBlob(const String & codec)
{
    PuffinBlob blob;
    blob.type = PUFFIN_THETA_SKETCH_BLOB_TYPE;
    blob.snapshot_id = 7;
    blob.sequence_number = 3;
    blob.fields.push_back(1);
    blob.compression_codec = codec;
    blob.properties.emplace("ndv", "42");
    return blob;
}

}

TEST(PuffinFile, WriteAndReadBlobs)
{
    const std::vector<std::pair<String, String>> codecs_and_bytes = {
        {"", "plain"},
        {"zstd", compress("zstd payload", CompressionMethod::Zstd)},
        {"lz4", compress("lz4 payload", CompressionMethod::Lz4)},
        {"snappy", "unknown codec"},
    };
    WriteBufferFromOwnString out;
    PuffinWriter writer(out);
    for (const auto & [codec, bytes] : codecs_and_bytes)
        writer.addBlob(makeBlob(codec), bytes);
    const size_t footer_size = writer.finalize();
    out.finalize();
    const String file = out.str();

    ReadBufferFromString in(file);
    PuffinReader reader(in);
    const auto & blobs = reader.getBlobs();
    ASSERT_EQ(blobs.size(), codecs_and_bytes.size());
    const auto & last = blobs.back();
    EXPECT_EQ(footer_size, file.size() - static_cast<size_t>(last.offset + last.length));

    const auto & first = blobs.front();
    EXPECT_EQ(first.type, PUFFIN_THETA_SKETCH_BLOB_TYPE);
    EXPECT_EQ(first.snapshot_id, 7);
    EXPECT_EQ(first.sequence_number, 3);
    EXPECT_EQ(first.fields, VectorWithMemoryTracking<Int32>{1});
    EXPECT_EQ(first.properties.at("ndv"), "42");

    /// The unknown codec gives nothing: the NDV statistics skip such a blob.
    const std::vector<std::optional<String>> expected = {"plain", "zstd payload", "lz4 payload", std::nullopt};
    for (size_t i = 0; i < blobs.size(); ++i)
    {
        EXPECT_EQ(blobs[i].compression_codec, codecs_and_bytes[i].first);
        EXPECT_EQ(reader.readBlob(blobs[i]), expected[i]) << "blob " << i;
    }
}

TEST(PuffinFile, BlobMetadataJSON)
{
    PuffinBlob blob = makeBlob("zstd");
    blob.offset = 4;
    blob.length = 10;

    const auto in_footer = blob.toJSON(/*in_footer=*/ true);
    EXPECT_EQ(in_footer->getValue<Int64>("offset"), 4);
    EXPECT_EQ(in_footer->getValue<Int64>("length"), 10);
    EXPECT_EQ(in_footer->getValue<String>("compression-codec"), "zstd");

    const auto in_table_metadata = blob.toJSON(/*in_footer=*/ false);
    EXPECT_FALSE(in_table_metadata->has("offset"));
    EXPECT_FALSE(in_table_metadata->has("length"));
    EXPECT_FALSE(in_table_metadata->has("compression-codec"));
    EXPECT_EQ(in_table_metadata->getValue<String>("type"), PUFFIN_THETA_SKETCH_BLOB_TYPE);
    EXPECT_EQ(in_table_metadata->getValue<Int64>("snapshot-id"), 7);
    EXPECT_EQ(in_table_metadata->getArray("fields")->getElement<Int32>(0), 1);
}

TEST(PuffinFile, DecompressedBlobSizeIsCapped)
{
    const String bomb = compress(String(PUFFIN_BLOB_MAX_DECOMPRESSED_SIZE + 1, '\0'), CompressionMethod::Zstd);
    WriteBufferFromOwnString out;
    PuffinWriter writer(out);
    writer.addBlob(makeBlob("zstd"), bomb);
    writer.finalize();
    out.finalize();

    ReadBufferFromString in(out.str());
    PuffinReader reader(in);
    const auto & blobs = reader.getBlobs();
    try
    {
        reader.readBlob(blobs.at(0));
        FAIL() << "Expected LIMIT_EXCEEDED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
}
