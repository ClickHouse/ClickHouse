#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
/// `CasXxh3Streamer.h` is the isolated xxHash wrapper (a system header): it gives us `Cas::xxh3_128_oneshot`
/// as an independent one-shot reference without pulling raw xxHash symbols (or their warnings) into
/// this test — see the header's own comment for the lz4-shadowing / `-Werror` reasons.
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasXxh3Streamer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <base/extended_types.h>
#include <base/hex.h>

#include <algorithm>
#include <string>

using namespace DB;
using namespace DB::Cas;

namespace
{

/// A deterministic, non-repeating-byte payload (not all-zero / all-same, so a byte-order or
/// endianness bug in either hash path would not accidentally cancel out).
std::string makePayload(size_t size)
{
    std::string s;
    s.reserve(size);
    for (size_t i = 0; i < size; ++i)
        s.push_back(static_cast<char>('a' + (i % 23)));
    return s;
}

}

TEST(CASBlobHasher, Xxh3StreamingMatchesOneShotAndBlobHashHexOneShot)
{
    const std::string payload = makePayload(10000);

    std::string sink_data;
    std::string streaming_hex;
    {
        WriteBufferFromString sink(sink_data);
        auto hashing = makeBlobHashingWriteBuffer(BlobHashAlgo::XXH3_128, sink);

        /// Feed the payload through several `write()` chunks to exercise the streaming state across
        /// multiple `nextImpl` flushes, not just a single call.
        size_t offset = 0;
        constexpr size_t chunk = 777;
        while (offset < payload.size())
        {
            const size_t n = std::min(chunk, payload.size() - offset);
            hashing->write(payload.data() + offset, n);
            offset += n;
        }

        streaming_hex = hashing->getHashHex();
        hashing->finalize();
        sink.finalize();
    }

    /// The passthrough forwarded every byte unchanged.
    EXPECT_EQ(sink_data, payload);
    EXPECT_EQ(streaming_hex.size(), 32u);

    /// xxh3 streaming == xxh3 one-shot (unlike cityHash128, xxh3's streaming digest is defined to
    /// agree with the one-shot digest -- see `ImplXXH3_128` in `Functions/FunctionsHashing.h`).
    UInt64 os_low = 0;
    UInt64 os_high = 0;
    Cas::xxh3_128_oneshot(payload.data(), payload.size(), os_low, os_high);
    const std::string one_shot_hex = getHexUIntLowercase(UInt128{os_low, os_high});
    EXPECT_EQ(streaming_hex, one_shot_hex);

    /// The one-shot re-hash helper must agree with both.
    EXPECT_EQ(blobHashHexOneShot(BlobHashAlgo::XXH3_128, payload), one_shot_hex);
}

TEST(CASBlobHasher, CityHash128ByteIdenticalToHashingWriteBuffer)
{
    /// Cover payloads both under and over one `DBMS_DEFAULT_HASHING_BLOCK_SIZE` (2048 B) hash block,
    /// plus exactly at the boundary, since the chunked convention only matters once a payload spans
    /// more than one block.
    for (const size_t size : {size_t(100), size_t(2000), size_t(2048), size_t(5000)})
    {
        SCOPED_TRACE(size);
        const std::string payload = makePayload(size);

        /// Reference: today's convention, `HashingWriteBuffer` used directly.
        std::string ref_sink_data;
        std::string ref_hex;
        {
            WriteBufferFromString ref_sink(ref_sink_data);
            HashingWriteBuffer ref_hashing(ref_sink);
            ref_hashing.write(payload.data(), payload.size());
            ref_hex = getHexUIntLowercase(ref_hashing.getHash());
            ref_hashing.finalize();
            ref_sink.finalize();
        }

        /// The selectable factory, defaulted to CityHash128 -- must be byte-identical.
        std::string sink_data;
        std::string hex;
        {
            WriteBufferFromString sink(sink_data);
            auto hashing = makeBlobHashingWriteBuffer(BlobHashAlgo::CityHash128, sink);
            hashing->write(payload.data(), payload.size());
            hex = hashing->getHashHex();
            hashing->finalize();
            sink.finalize();
        }

        EXPECT_EQ(hex, ref_hex);
        EXPECT_EQ(hex.size(), 32u);
        EXPECT_EQ(sink_data, ref_sink_data);
        EXPECT_EQ(sink_data, payload);

        /// The one-shot re-hash helper must agree too.
        EXPECT_EQ(blobHashHexOneShot(BlobHashAlgo::CityHash128, payload), ref_hex);
    }
}

TEST(CASBlobHasher, AlgoNameAndParseRoundTrip)
{
    EXPECT_EQ(blobHashAlgoName(BlobHashAlgo::CityHash128), "ch128");
    EXPECT_EQ(blobHashAlgoName(BlobHashAlgo::XXH3_128), "xxh3");
    EXPECT_EQ(blobHashAlgoName(BlobHashAlgo::Sha256), "sha256");

    EXPECT_EQ(parseBlobHashAlgo("cityhash128"), BlobHashAlgo::CityHash128);
    EXPECT_EQ(parseBlobHashAlgo("xxh3-128"), BlobHashAlgo::XXH3_128);
    /// Parses even though it is rejected downstream (config-layer rejection is a later task).
    EXPECT_EQ(parseBlobHashAlgo("sha256"), BlobHashAlgo::Sha256);

    EXPECT_THROW(parseBlobHashAlgo("bogus"), DB::Exception);
    EXPECT_THROW(parseBlobHashAlgo("cityHash128"), DB::Exception); // case-sensitive
    EXPECT_THROW(parseBlobHashAlgo(""), DB::Exception);
}

TEST(CASBlobHasher, Sha256OneShotGoldenVectors)
{
    /// NIST/FIPS 180-2 test vectors, the standard SHA-256 sanity check.
    EXPECT_EQ(blobHashHexOneShot(BlobHashAlgo::Sha256, "abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(blobHashHexOneShot(BlobHashAlgo::Sha256, ""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(CASBlobHasher, Sha256StreamingMatchesOneShotAndIsPassthrough)
{
    /// Bigger than `DBMS_DEFAULT_HASHING_BLOCK_SIZE` (2048 B) so the payload chunks through several
    /// `nextImpl` flushes, not just a single call.
    const std::string payload = makePayload(200 * 1024);

    std::string sink_data;
    std::string streaming_hex;
    {
        WriteBufferFromString sink(sink_data);
        auto hashing = makeBlobHashingWriteBuffer(BlobHashAlgo::Sha256, sink);

        /// Feed the payload through several `write()` chunks to exercise the streaming EVP digest
        /// across multiple `nextImpl` flushes.
        size_t offset = 0;
        constexpr size_t chunk = 4096;
        while (offset < payload.size())
        {
            const size_t n = std::min(chunk, payload.size() - offset);
            hashing->write(payload.data() + offset, n);
            offset += n;
        }

        streaming_hex = hashing->getHashHex();
        hashing->finalize();
        sink.finalize();
    }

    /// The passthrough forwarded every byte unchanged.
    EXPECT_EQ(sink_data, payload);
    EXPECT_EQ(streaming_hex.size(), 64u);

    /// SHA-256 streaming == SHA-256 one-shot (like xxh3, unlike cityHash128 -- SHA-256 has no
    /// chunked convention to preserve).
    const std::string one_shot_hex = blobHashHexOneShot(BlobHashAlgo::Sha256, payload);
    EXPECT_EQ(streaming_hex, one_shot_hex);
}
