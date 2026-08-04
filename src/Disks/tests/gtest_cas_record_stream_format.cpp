#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>
#include <vector>

using namespace DB;
using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{

BlobRef chRef(uint64_t n)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(n))};
}

SourceEdgeRecord edge(const BlobRef & ref, uint64_t source_id)
{
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(source_id), .marker = kEdgeActive};
}

SourceEdgeRecord zero(const BlobRef & ref)
{
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(0), .marker = kZeroMarker};
}

SourceEdgeRecord condemned(const BlobRef & ref, const Token & token, uint64_t size, uint64_t round, bool pend)
{
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(0), .marker = kCondemned,
                            .delete_pending = pend, .token = token, .size = size, .condemn_round = round};
}

/// Encode a run from records already in (ref, source_id) order.
String encodeRun(const std::vector<SourceEdgeRecord> & recs)
{
    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    for (const auto & r : recs)
        writer.append(r);
    writer.finish();
    out.finalize();
    return out.str();
}

/// Stream a run back to records; verifies the trailer count as a side effect.
std::vector<SourceEdgeRecord> decodeRun(const String & bytes)
{
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    SourceEdgeRunReader reader(in);
    std::vector<SourceEdgeRecord> out;
    SourceEdgeRecord r;
    while (reader.next(r))
        out.push_back(r);
    return out;
}

}

TEST(CASRecordStream, EmptyRunRoundTripsAndChecksumMatches)
{
    const String bytes = encodeRun({});
    EXPECT_EQ(bytes, fmt::format(
        "{{\"type\":\"cas_run\",\"v\":{},\"kind\":\"source_edge\"}}\n{{\"n\":0}}\n", currentCompatibilityVersion()));

    ReadBufferFromMemory in(bytes.data(), bytes.size());
    SourceEdgeRunReader reader(in);
    SourceEdgeRecord r;
    EXPECT_FALSE(reader.next(r));
    /// The read-side accumulated hash equals the write-side helper over the same bytes.
    reader.verifyAgainst(sourceEdgeRunChecksum(bytes));
}

TEST(CASRecordStream, EdgeZeroCondemnedRoundTrip)
{
    const BlobRef a = chRef(1);
    const BlobRef b = chRef(2);
    const BlobRef c = chRef(3);
    /// Sorted by (ref, source_id): b's condemned sentinel is at source_id 0 (sorts first for b); a has
    /// an edge; c has a zero marker. Blobs ascend a < b < c, so the sequence is already non-decreasing.
    std::vector<SourceEdgeRecord> recs = {
        edge(a, 10),
        condemned(b, Token{"e-1", TokenType::ETag}, 4242, 7, /*pend*/ true),
        zero(c),
    };
    const String bytes = encodeRun(recs);
    const std::vector<SourceEdgeRecord> back = decodeRun(bytes);
    ASSERT_EQ(back.size(), 3u);

    EXPECT_EQ(back[0].ref, a);
    EXPECT_EQ(back[0].source_id, UInt128(10));
    EXPECT_EQ(back[0].marker, kEdgeActive);

    EXPECT_EQ(back[1].ref, b);
    EXPECT_EQ(back[1].source_id, UInt128(0));
    EXPECT_EQ(back[1].marker, kCondemned);
    EXPECT_TRUE(back[1].delete_pending);
    EXPECT_EQ(back[1].token, (Token{"e-1", TokenType::ETag}));
    EXPECT_EQ(back[1].size, 4242u);
    EXPECT_EQ(back[1].condemn_round, 7u);

    EXPECT_EQ(back[2].ref, c);
    EXPECT_EQ(back[2].marker, kZeroMarker);
}

TEST(CASRecordStream, WriterIsByteDeterministic)
{
    std::vector<SourceEdgeRecord> recs = {
        edge(chRef(1), 5),
        edge(chRef(1), 9),
        condemned(chRef(2), Token{"t/with/slashes", TokenType::ETag}, 1, 2, false),
    };
    EXPECT_EQ(encodeRun(recs), encodeRun(recs));   /// pure function of the sorted record set
}

TEST(CASRecordStream, SortOrderAcrossAlgosFollowsAlgoByte)
{
    /// b = <algo byte 2-hex><digest hex>. The algo byte leads, so string-sorting b reproduces the
    /// binary (algo, digest, source_id) order: ch128 (01) < xxh3 (02) < sha256 (03).
    BlobDigest d16 = BlobDigest::fromU128(UInt128(7));
    BlobDigest d32{};
    d32.bytes[0] = 0x10;
    const BlobRef ch{BlobHashAlgo::CityHash128, d16};
    const BlobRef xx{BlobHashAlgo::XXH3_128, d16};
    const BlobRef sha{BlobHashAlgo::Sha256, d32};

    /// Accepted in algo-byte order without an out-of-order throw.
    const String bytes = encodeRun({edge(ch, 1), edge(xx, 1), edge(sha, 1)});
    const std::vector<SourceEdgeRecord> back = decodeRun(bytes);
    ASSERT_EQ(back.size(), 3u);
    EXPECT_EQ(back[0].ref.algo, BlobHashAlgo::CityHash128);
    EXPECT_EQ(back[1].ref.algo, BlobHashAlgo::XXH3_128);
    EXPECT_EQ(back[2].ref.algo, BlobHashAlgo::Sha256);
}

TEST(CASRecordStream, AppendOutOfOrderThrows)
{
    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    writer.append(edge(chRef(2), 1));
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            writer.append(edge(chRef(1), 1));
        },
        "records appended out of");   /// ref regression
}

TEST(CASRecordStream, SourceIdRendersAs32Hex)
{
    const String bytes = encodeRun({edge(chRef(1), 10)});
    /// The source id 10 is a 32-char lowercase hex string ending in 'a'.
    EXPECT_NE(bytes.find("\"s\":\"0000000000000000000000000000000a\""), String::npos);
    /// The record key `b` for a ch128 ref is the algo byte 01 + a 32-hex digest (34 chars total).
    EXPECT_NE(bytes.find("\"b\":\"01"), String::npos);
}

TEST(CASRecordStream, SealChecksumMismatchFailsClosed)
{
    const String bytes = encodeRun({edge(chRef(1), 10), edge(chRef(1), 20)});
    const UInt128 good = sourceEdgeRunChecksum(bytes);

    /// A correct verify passes.
    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        SourceEdgeRunReader reader(in);
        SourceEdgeRecord r;
        while (reader.next(r)) {}
        reader.verifyAgainst(good);
    }

    /// Any byte flip either fails the parse or the whole-file checksum — never silently trusted.
    String flipped = bytes;
    flipped[flipped.size() / 2] ^= 0x20;
    EXPECT_NE(sourceEdgeRunChecksum(flipped), good);
    EXPECT_THROW({
        ReadBufferFromMemory in(flipped.data(), flipped.size());
        SourceEdgeRunReader reader(in);
        SourceEdgeRecord r;
        while (reader.next(r)) {}
        reader.verifyAgainst(good);
    }, DB::Exception);
}

TEST(CASRecordStream, TrailerCountMismatchIsCorruptData)
{
    String bytes = encodeRun({edge(chRef(1), 10)});
    /// Rewrite the trailer count 1 -> 2.
    const String from = "{\"n\":1}\n";
    const String to = "{\"n\":2}\n";
    const size_t at = bytes.rfind(from);
    ASSERT_NE(at, String::npos);
    bytes.replace(at, from.size(), to);
    EXPECT_THROW(decodeRun(bytes), DB::Exception);
}

TEST(CASRecordStream, TruncationAtLineBoundaryFailsClosed)
{
    const String bytes = encodeRun({edge(chRef(1), 10), edge(chRef(1), 20)});
    /// Drop the trailer line entirely (truncate after the last record's newline).
    const size_t trailer = bytes.rfind("{\"n\":");
    ASSERT_NE(trailer, String::npos);
    EXPECT_THROW(decodeRun(bytes.substr(0, trailer)), DB::Exception);
}

TEST(CASRecordStream, HeaderGates)
{
    /// Wrong type.
    {
        const String s = "{\"type\":\"cas_pool_meta\",\"v\":3,\"kind\":\"source_edge\"}\n{\"n\":0}\n";
        EXPECT_THROW(decodeRun(s), DB::Exception);
    }
    /// Wrong kind.
    {
        const String s = "{\"type\":\"cas_run\",\"v\":3,\"kind\":\"blob_delta\"}\n{\"n\":0}\n";
        EXPECT_THROW(decodeRun(s), DB::Exception);
    }
    /// Future version -> UNKNOWN_FORMAT_VERSION.
    {
        const String s = fmt::format(
            "{{\"type\":\"cas_run\",\"v\":{},\"kind\":\"source_edge\"}}\n{{\"n\":0}}\n", currentCompatibilityVersion() + 1);
        ReadBufferFromMemory in(s.data(), s.size());
        try
        {
            SourceEdgeRunReader reader(in);
            FAIL() << "expected UNKNOWN_FORMAT_VERSION";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION);
        }
    }
    /// An out-of-range version must not narrow to a valid low u32 value.
    {
        const String s = "{\"type\":\"cas_run\",\"v\":4294967299,\"kind\":\"source_edge\"}\n{\"n\":0}\n";
        try
        {
            decodeRun(s);
            FAIL() << "expected CORRUPTED_DATA";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        }
    }
}
