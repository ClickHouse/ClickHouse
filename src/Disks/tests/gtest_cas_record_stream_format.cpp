#include <gtest/gtest.h>
#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>
#include <algorithm>
#include <vector>

#include <magic_enum.hpp>

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
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(source_id), .marker = RunMarker::Edge};
}

SourceEdgeRecord zero(const BlobRef & ref)
{
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(0), .marker = RunMarker::Zero};
}

SourceEdgeRecord condemned(const BlobRef & ref, const PersistedEtag & token, uint64_t size, uint64_t round, bool pend)
{
    return SourceEdgeRecord{.ref = ref, .source_id = UInt128(0), .marker = RunMarker::Condemned,
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

CAS_BATTERY_COVERS(RunFile);

TEST(CASFormatBattery, RunFile)
{
    const std::vector<SourceEdgeRecord> records{edge(chRef(2), 5)};
    runFormatBattery({FormatId::RunFile,
        [&] { return sealObject(FormatId::RunFile, encodeRun(records)); },
        [](std::string_view s) { decodeRun(std::string(openObject(FormatId::RunFile, s))); },
        fmt::format("{{\"type\":\"cas_run\",\"v\":{},\"kind\":\"source_edge\"}}\n", currentCompatibilityVersion()) +
        "{\"ref\":\"0100000000000000000000000000000002\",\"src\":\"00000000000000000000000000000005\",\"mark\":\"edge\"}\n"
        "{\"n\":1}\n"});
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
        condemned(b, PersistedEtag{"etag", "e-1"}, 4242, 7, /*pend*/ true),
        zero(c),
    };
    const String bytes = encodeRun(recs);
    const std::vector<SourceEdgeRecord> back = decodeRun(bytes);
    ASSERT_EQ(back.size(), 3u);

    EXPECT_EQ(back[0].ref, a);
    EXPECT_EQ(back[0].source_id, UInt128(10));
    EXPECT_EQ(back[0].marker, RunMarker::Edge);

    EXPECT_EQ(back[1].ref, b);
    EXPECT_EQ(back[1].source_id, UInt128(0));
    EXPECT_EQ(back[1].marker, RunMarker::Condemned);
    EXPECT_TRUE(back[1].delete_pending);
    EXPECT_EQ(back[1].token.dialect, "etag");
    EXPECT_EQ(back[1].token.value, "e-1");
    EXPECT_EQ(back[1].size, 4242u);
    EXPECT_EQ(back[1].condemn_round, 7u);

    EXPECT_EQ(back[2].ref, c);
    EXPECT_EQ(back[2].marker, RunMarker::Zero);
}

/// Closed-set pin: the three `RunMarker` words, walked through `magic_enum::enum_values`, which is what proves the
/// renderer and the parser consult the SAME table: a table entry missing altogether is already a
/// build error at the coverage assert, but two delegates drifting onto different tables is not.
TEST(CASRecordStream, ClosedSetPinsRunMarkerWords)
{
    EXPECT_EQ(runMarkerToWireWord(RunMarker::Zero), "zero");
    EXPECT_EQ(runMarkerToWireWord(RunMarker::Edge), "edge");
    EXPECT_EQ(runMarkerToWireWord(RunMarker::Condemned), "condemned");
    for (const auto m : magic_enum::enum_values<RunMarker>())
        EXPECT_EQ(runMarkerFromWireWord(runMarkerToWireWord(m)), m);
}

/// The condemned row's six fields are all-or-nothing: a row that says `condemned` but drops one of
/// them would decode with a silently defaulted value (a zero size, an empty token, `pending` false),
/// which is a different retention decision than the writer recorded.
TEST(CASRecordStream, CondemnedRowMissingOneOfItsSixFieldsFailsClosed)
{
    const String good = encodeRun({condemned(chRef(2), PersistedEtag{"etag", "e-1"}, 4242, 7, /*pend*/ true)});
    for (const std::string_view field : {R"(,"pending":true)", R"(,"token_type":"etag")", R"(,"token":"e-1")",
                                         R"(,"size":4242)", R"(,"condemn_round":"7")", R"(,"confirmed":false)"})
    {
        String bytes = good;
        const size_t at = bytes.find(field);
        ASSERT_NE(at, String::npos) << "fixture does not carry " << field;
        bytes.erase(at, field.size());
        try
        {
            static_cast<void>(decodeRun(bytes));
            FAIL() << "expected CORRUPTED_DATA after dropping " << field;
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
            const String expected_message = field == R"(,"token_type":"etag")" || field == R"(,"token":"e-1")"
                ? "CAS cas_run: token missing token_type/token"
                : "CAS cas_run: condemned record missing pending/size/condemn_round/confirmed";
            EXPECT_EQ(e.message(), expected_message);
        }
    }
}

/// The mirror fence: an active row carrying any condemned field is a row whose two halves disagree
/// about what it is, and the reader must not pick one half.
TEST(CASRecordStream, ActiveRowCarryingACondemnedFieldFailsClosed)
{
    String bytes = encodeRun({edge(chRef(1), 10)});
    const String needle = R"(,"mark":"edge")";
    const size_t at = bytes.find(needle);
    ASSERT_NE(at, String::npos);
    bytes.insert(at + needle.size(), R"(,"size":4242)");

    try
    {
        static_cast<void>(decodeRun(bytes));
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_EQ(e.message(), "CAS cas_run: non-condemned record carries condemned fields");
    }
}

/// `CasBlobInDegree.cpp`'s fold (`SourceEdgeRunWriter writer(out); // sorted NDJSON; byte-deterministic
/// for write-once adoption`) sorts by `(ref, source_id)` before writing, so the run this fixture emits
/// is the writer's own straight-line append, never a reorder -- the property the run must actually have
/// is that whichever order the SAME set of edges was DISCOVERED in, the canonical `(ref, source_id)`
/// sort before the writer sees them converges on identical bytes. A bare `f(x) == f(x)` on one fixed,
/// already-sorted vector cannot fail on anything but genuine cross-call nondeterminism (a clock, a
/// pointer, hash randomization) -- none of which this format has -- so it passed vacuously; two
/// differently-DISCOVERED inputs, sorted by the same comparator production uses, is the real claim.
TEST(CASRecordStream, WriterIsByteDeterministic)
{
    const auto byRefThenSource = [](const SourceEdgeRecord & a, const SourceEdgeRecord & b)
    {
        if (a.ref != b.ref)
            return a.ref < b.ref;
        return a.source_id < b.source_id;
    };

    std::vector<SourceEdgeRecord> discovered_ascending = {
        edge(chRef(1), 5),
        edge(chRef(1), 9),
        condemned(chRef(2), PersistedEtag{"etag", "t/with/slashes"}, 1, 2, false),
    };
    /// The SAME three edges, as if a different GC shard or a different LIST page order had surfaced
    /// them: reverse discovery order, still every one of them present.
    std::vector<SourceEdgeRecord> discovered_reverse = {
        condemned(chRef(2), PersistedEtag{"etag", "t/with/slashes"}, 1, 2, false),
        edge(chRef(1), 9),
        edge(chRef(1), 5),
    };
    ASSERT_NE(discovered_ascending.front().ref, discovered_reverse.front().ref)
        << "the two discovery orders must actually differ";

    std::sort(discovered_ascending.begin(), discovered_ascending.end(), byRefThenSource);
    std::sort(discovered_reverse.begin(), discovered_reverse.end(), byRefThenSource);
    EXPECT_EQ(encodeRun(discovered_ascending), encodeRun(discovered_reverse))
        << "the run must be a pure function of the edge SET, not of the order it was discovered in";
}

/// The run `ref` carries the algorithm as a raw leading BYTE, a second representation of the same
/// closed set the `algo` WORD spells elsewhere. The word side is proven exhaustive at compile time by
/// its wire table; the byte side is a hand-written switch, so nothing but this walk stops a new
/// algorithm from being written by `renderB` and rejected by the reader -- an asymmetry that would
/// appear as unreadable runs rather than as a failing build.
TEST(CASRecordStream, EveryBlobHashAlgoRoundTripsThroughTheRunRefByte)
{
    for (const BlobHashAlgo algo : magic_enum::enum_values<BlobHashAlgo>())
    {
        BlobDigest digest{};
        digest.bytes[0] = 0x10;
        const BlobRef ref{algo, digest};
        const std::vector<SourceEdgeRecord> back = decodeRun(encodeRun({edge(ref, 1)}));
        ASSERT_EQ(back.size(), 1u) << "algo " << magic_enum::enum_name(algo);
        EXPECT_EQ(back[0].ref.algo, algo) << "the leading byte did not survive the round trip";
    }
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
    EXPECT_NE(bytes.find("\"src\":\"0000000000000000000000000000000a\""), String::npos);
    /// The record key `ref` for a ch128 ref is the algo byte 01 + a 32-hex digest (34 chars total).
    EXPECT_NE(bytes.find("\"ref\":\"01"), String::npos);
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

TEST(CASRecordStream, UppercaseDigestInRecordKeyIsCorruptedData)
{
    String bytes = encodeRun({edge(chRef(10), 1)});
    const size_t digest = bytes.find("0000000000000000000000000000000a");
    ASSERT_NE(digest, String::npos);
    bytes[digest + 31] = 'A';

    try
    {
        static_cast<void>(decodeRun(bytes));
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
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
        const String s = "{\"type\":\"cas_pool_meta\",\"v\":1,\"kind\":\"source_edge\"}\n{\"n\":0}\n";
        EXPECT_THROW(decodeRun(s), DB::Exception);
    }
    /// Wrong kind. `v:1` is the baseline generation, so it always passes the header gate before the
    /// kind check runs.
    {
        const String s = "{\"type\":\"cas_run\",\"v\":1,\"kind\":\"blob_delta\"}\n{\"n\":0}\n";
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
