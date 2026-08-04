#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <utility>

using namespace DB::Cas;

namespace
{

/// NOT `Disks/tests/cas_test_helpers.h`'s `DB::Cas::tests::expectThrowsCode`: pulling in that header
/// drags along a large chunk of the CAS backend/store machinery this file has no other need for, so it
/// stays clear of `cas_test_helpers.h` entirely and inlines its own copy of the same tiny assertion
/// instead.
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

/// One Blob + one Inline entry, matching the plan's §text-shape illustration verbatim (codecs-v3
/// phase 6): deliberately NOT path-sorted on input, so the round trip also exercises canonical
/// path-order encoding.
PartManifest sample()
{
    PartManifest m;
    m.ref = ManifestRef{5, 15, 1};
    m.root_namespace_id = RootNamespace("00/aa@cas@");

    ManifestEntry inl;
    inl.path = "c/small.txt";
    inl.placement = EntryPlacement::Inline;
    inl.inline_bytes = "hello world!";   /// 12 raw bytes, no embedded '\n'

    ManifestEntry blob;
    blob.path = "a/b.bin";
    blob.placement = EntryPlacement::Blob;
    blob.ref = BlobRef{BlobHashAlgo::CityHash128, codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
    blob.blob_size = 4096;

    m.entries = {inl, blob};   /// deliberately out of canonical order
    /// Set LAST, after all other fields (matches gtest_cas_manifest_codec.cpp's
    /// makeTwoEntryManifestForOrderTest): decode now recomputes + verifies this, so a placeholder
    /// value here would make every test that round-trips `sample()` through decode fail closed.
    m.payload_digest = computePayloadDigest(m);
    return m;
}

}

TEST(CASFormatBattery, PartManifest)
{
    const PartManifest m = sample();
    /// Interpolate the REAL digest (never hand-compute a CityHash128 hex by hand) so the golden text
    /// stays self-consistent with whatever sample() produces, now that decode verifies payload_digest.
    const String golden =
        currentFormatHeader("cas_part_manifest") +
        "{\"me\":\"5\",\"mb\":\"15\",\"mo\":1,\"ns\":\"00/aa@cas@\",\"pd\":\"" + u128ToHex(m.payload_digest) + "\"}\n" // NOLINT(modernize-raw-string-literal): mixes '\"' quoting with '\n' line endings across this concatenated literal; a raw string can't hold the newline as-is.
        "{\"p\":\"a/b.bin\",\"pm\":\"blob\",\"ha\":\"ch128\",\"h\":\"00112233445566778899aabbccddeeff\",\"sz\":4096}\n"
        "{\"p\":\"c/small.txt\",\"pm\":\"inline\",\"il\":12}\n"
        "{\"n\":2}\n"
        "==> c/small.txt il=12 <==\n"
        "hello world!\n";
    runFormatBattery({FormatId::PartManifest,
        [&] { return sealObject(FormatId::PartManifest, encodePartManifest(m)); },
        [](std::string_view d) { decodePartManifest(std::string(openObject(FormatId::PartManifest, d))); },
        golden});
}

TEST(CASPartManifestFormat, RoundTripDescriptorAndEntries)
{
    const PartManifest m = sample();
    const PartManifest got = decodePartManifest(encodePartManifest(m));
    EXPECT_EQ(got.ref, m.ref);
    EXPECT_EQ(got.root_namespace_id, m.root_namespace_id);
    EXPECT_EQ(got.payload_digest, m.payload_digest);
    ASSERT_EQ(got.entries.size(), 2u);

    /// canonical path order: "a/b.bin" < "c/small.txt"
    EXPECT_EQ(got.entries[0].path, "a/b.bin");
    EXPECT_EQ(got.entries[0].placement, EntryPlacement::Blob);
    EXPECT_EQ(got.entries[0].ref, m.entries[1].ref);
    EXPECT_EQ(got.entries[0].blob_size, 4096u);

    EXPECT_EQ(got.entries[1].path, "c/small.txt");
    EXPECT_EQ(got.entries[1].placement, EntryPlacement::Inline);
    /// The payload-zone round trip: exact raw bytes recovered from the banner+bytes+'\n' zone.
    EXPECT_EQ(got.entries[1].inline_bytes, "hello world!");
}

TEST(CASPartManifestFormat, EmptyEntriesRoundTrips)
{
    PartManifest m = sample();
    m.entries.clear();
    m.payload_digest = computePayloadDigest(m);   /// recompute: content changed, sample()'s digest is stale
    const PartManifest got = decodePartManifest(encodePartManifest(m));
    EXPECT_TRUE(got.entries.empty());
    EXPECT_EQ(got.ref, m.ref);
    /// No payload zone at all when there are no Inline entries.
    EXPECT_FALSE(encodePartManifest(m).contains("==>"));
}

TEST(CASPartManifestFormat, PlacementWordsRenderAndRejectUnknown)
{
    const String text = encodePartManifest(sample());
    EXPECT_NE(text.find("\"pm\":\"blob\""), String::npos);
    EXPECT_NE(text.find("\"pm\":\"inline\""), String::npos);

    /// An unknown placement word fails closed.
    String bad = text;
    const size_t pos = bad.find(R"("pm":"blob")");
    ASSERT_NE(pos, String::npos);
    bad.replace(pos, String(R"("pm":"blob")").size(), R"("pm":"bogus")");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(bad); });
}

/// Proves the payload zone, not JSON-string escaping: an Inline entry whose bytes contain an
/// embedded '\n', a NUL byte, and a '"' character round-trip byte-faithfully. If this content were
/// carried as a JSON string value it would need escaping (or would be flatly invalid for the NUL
/// byte); the payload zone instead carries it as raw length-delimited bytes.
TEST(CASPartManifestFormat, InlineBytesWithEmbeddedSpecialCharsRoundTripByteFaithfully)
{
    PartManifest m;
    m.ref = ManifestRef{7, 21, 2};
    m.root_namespace_id = RootNamespace("00/bb@cas@");

    ManifestEntry e;
    e.path = "weird.bin";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "line1\nline2";
    e.inline_bytes.push_back('\0');
    e.inline_bytes += "after-nul\"quoted\"end";
    m.entries = {e};
    m.payload_digest = computePayloadDigest(m);

    const PartManifest got = decodePartManifest(encodePartManifest(m));
    ASSERT_EQ(got.entries.size(), 1u);
    EXPECT_EQ(got.entries[0].inline_bytes, m.entries[0].inline_bytes);
    EXPECT_EQ(got.entries[0].inline_bytes.size(), e.inline_bytes.size());
}

TEST(CASPartManifestFormat, ByteDeterminism)
{
    const PartManifest m = sample();
    /// Encode twice -> identical bytes. Also encode a copy with entries pre-shuffled into the other
    /// order -> still identical, because the encoder sorts canonically.
    PartManifest m2 = m;
    std::swap(m2.entries[0], m2.entries[1]);
    EXPECT_EQ(encodePartManifest(m), encodePartManifest(m));
    EXPECT_EQ(encodePartManifest(m), encodePartManifest(m2));
}

TEST(CASPartManifestFormat, MixedAlgoEntriesRoundTrip)
{
    PartManifest m;
    m.ref = ManifestRef{9, 33, 4};
    m.root_namespace_id = RootNamespace("00/cc@cas@");

    ManifestEntry e16;
    e16.path = "a/ch128.bin";
    e16.placement = EntryPlacement::Blob;
    e16.ref = BlobRef{BlobHashAlgo::CityHash128,
        codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
    e16.blob_size = 100;

    ManifestEntry e32;
    e32.path = "b/sha256.bin";
    e32.placement = EntryPlacement::Blob;
    e32.ref = BlobRef{BlobHashAlgo::Sha256, codecFor(BlobHashAlgo::Sha256).fromHex(String(64, 'a'))};
    e32.blob_size = 200;

    m.entries = {e16, e32};
    m.payload_digest = computePayloadDigest(m);

    const PartManifest got = decodePartManifest(encodePartManifest(m));
    ASSERT_EQ(got.entries.size(), 2u);
    EXPECT_EQ(got.entries[0].path, "a/ch128.bin");
    EXPECT_EQ(got.entries[0].ref, e16.ref);
    EXPECT_EQ(got.entries[0].blob_size, 100u);
    EXPECT_EQ(got.entries[1].path, "b/sha256.bin");
    EXPECT_EQ(got.entries[1].ref, e32.ref);
    EXPECT_EQ(got.entries[1].blob_size, 200u);
}

/// Builds a single-Blob-entry manifest whose entry path is exactly `path` -- `encodePartManifest`
/// itself does not validate path shape (only ordering/duplicates), so this lets the negative cases
/// below reach `decodePartManifest`'s shape check unobstructed.
static PartManifest manifestWithSinglePath(std::string_view path)
{
    PartManifest m;
    m.ref = ManifestRef{17, 66, 7};
    m.root_namespace_id = RootNamespace("00/ff@cas@");

    ManifestEntry e;
    e.path = String(path);
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128,
        codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
    e.blob_size = 10;
    m.entries = {e};
    m.payload_digest = computePayloadDigest(m);
    return m;
}

/// T11: manifest bytes arrive over the interserver relink channel, so decode enforces the same path
/// hygiene as CasLayout::checkNamespace -- relative, no empty/'.'/'..' segments, no leading '/'.
/// `encodePartManifest` does not itself reject these (see `manifestWithSinglePath`), so each case
/// must fail closed at decode time instead.
TEST(CASPartManifestFormat, DecodeRejectsMalformedEntryPaths)
{
    for (const char * path : {"../evil", "/abs", "", "a//b", "a/./b"})
    {
        SCOPED_TRACE(path);
        const String encoded = encodePartManifest(manifestWithSinglePath(path));
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(encoded); });
    }
}

/// Legal projection subdirectories (`<projection>.proj/<file>`) must not be caught by the shape
/// check above -- it is syntactic only, not a directory-depth restriction.
TEST(CASPartManifestFormat, DecodeAcceptsLegalProjectionSubdirPath)
{
    const PartManifest m = manifestWithSinglePath("proj.proj/data.bin");
    const PartManifest got = decodePartManifest(encodePartManifest(m));
    ASSERT_EQ(got.entries.size(), 1u);
    EXPECT_EQ(got.entries[0].path, "proj.proj/data.bin");
}

TEST(CASPartManifestFormat, DuplicatePathRejectedOnEncode)
{
    PartManifest m = sample();
    ManifestEntry dup = m.entries[0];   /// same path as an existing entry
    m.entries.push_back(dup);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodePartManifest(m); });
}

/// Hand-forge two valid entry-record LINES swapped out of canonical order (no CRC-patching forge
/// helpers needed - this is a text format, lines carry no per-line checksum). Both entries are Blob
/// (no payload-zone bytes), so the swap cannot disturb payload-zone alignment - it isolates exactly
/// the ordering check.
TEST(CASPartManifestFormat, DecodeRejectsOutOfOrderEntries)
{
    PartManifest m;
    m.ref = ManifestRef{11, 44, 5};
    m.root_namespace_id = RootNamespace("00/dd@cas@");

    auto mkBlob = [](std::string_view path)
    {
        ManifestEntry e;
        e.path = String(path);
        e.placement = EntryPlacement::Blob;
        e.ref = BlobRef{BlobHashAlgo::CityHash128,
            codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
        e.blob_size = 10;
        return e;
    };
    /// "a/one.bin" and "b/two.bin" are the same length, so swapping their record lines in place
    /// does not shift any other byte offset in the text.
    m.entries = {mkBlob("a/one.bin"), mkBlob("b/two.bin"), mkBlob("c/three.bin")};
    m.payload_digest = computePayloadDigest(m);

    const String text = encodePartManifest(m);
    const size_t pos_a = text.find(R"("p":"a/one.bin")");
    const size_t pos_b = text.find(R"("p":"b/two.bin")");
    ASSERT_NE(pos_a, String::npos);
    ASSERT_NE(pos_b, String::npos);

    const size_t a_start = text.rfind('\n', pos_a) + 1;
    const size_t a_end = text.find('\n', pos_a) + 1;
    const size_t b_start = text.rfind('\n', pos_b) + 1;
    const size_t b_end = text.find('\n', pos_b) + 1;
    const String a_line = text.substr(a_start, a_end - a_start);
    const String b_line = text.substr(b_start, b_end - b_start);
    ASSERT_EQ(a_line.size(), b_line.size());

    String forged = text;
    forged.replace(a_start, a_line.size(), b_line);
    forged.replace(b_start, b_line.size(), a_line);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(forged); });
}

/// a < b < c in canonical order; forge entry c's path to equal entry a's path. A naive "only check
/// adjacent pairs" implementation would miss this (c is only ever compared against b, never against
/// a); requiring strict ascending order against just the immediately-preceding entry still catches
/// it, because the forged c(=a's path) is no longer greater than b either.
TEST(CASPartManifestFormat, DecodeRejectsNonAdjacentDuplicatePath)
{
    PartManifest m;
    m.ref = ManifestRef{13, 55, 6};
    m.root_namespace_id = RootNamespace("00/ee@cas@");

    auto mkBlob = [](std::string_view path)
    {
        ManifestEntry e;
        e.path = String(path);
        e.placement = EntryPlacement::Blob;
        e.ref = BlobRef{BlobHashAlgo::CityHash128,
            codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
        e.blob_size = 10;
        return e;
    };
    m.entries = {mkBlob("aaa/one.bin"), mkBlob("bbb/two.bin"), mkBlob("ccc/three.bin")};
    m.payload_digest = computePayloadDigest(m);

    String forged = encodePartManifest(m);
    const String needle = R"("p":"ccc/three.bin")";
    const size_t pos = forged.find(needle);
    ASSERT_NE(pos, String::npos);
    forged.replace(pos, needle.size(), R"("p":"aaa/one.bin")");

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(forged); });
}

TEST(CASPartManifestFormat, UnknownEntryAlgoFailsClosed)
{
    String bad = encodePartManifest(sample());
    const String needle = R"("ha":"ch128")";
    const size_t pos = bad.find(needle);
    ASSERT_NE(pos, String::npos);
    bad.replace(pos, needle.size(), R"("ha":"bogus")");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(bad); });
}

/// `DigestCodec::fromHex` throws BAD_ARGUMENTS (not CORRUPTED_DATA) on a width mismatch; decode must
/// check the width itself first so this fails closed with the same code every other decode error
/// here uses.
TEST(CASPartManifestFormat, DigestHexWidthMismatchFailsClosedNotBadArguments)
{
    String bad = encodePartManifest(sample());
    const String key = R"("h":")";
    const size_t key_pos = bad.find(key);
    ASSERT_NE(key_pos, String::npos);
    const size_t hex_start = key_pos + key.size();
    const size_t hex_end = bad.find('"', hex_start);
    ASSERT_NE(hex_end, String::npos);
    ASSERT_EQ(hex_end - hex_start, 32u);   /// ch128: 16-byte digest -> 32 hex chars
    bad.erase(hex_start, 1);               /// drop one hex char -> width mismatch (31 chars)
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(bad); });
}

/// Pure-function properties of computePayloadDigest, independent of decode-time verification: stable
/// across calls for identical content, independent of the payload_digest field's own value, and
/// content-sensitive (changes when real content changes).
TEST(CASPartManifestFormat, PayloadDigestStableAndContentSensitive)
{
    const PartManifest m = sample();
    PartManifest with_different_stored_digest = m;
    with_different_stored_digest.payload_digest = UInt128(0x1234);
    EXPECT_EQ(computePayloadDigest(m), computePayloadDigest(m));
    EXPECT_EQ(computePayloadDigest(m), computePayloadDigest(with_different_stored_digest));

    /// m.entries[1] is the Blob entry (m.entries[0] is Inline, whose blob_size is unused on the
    /// wire) - changing its blob_size changes the canonical encoding and therefore the digest.
    ASSERT_EQ(m.entries[1].placement, EntryPlacement::Blob);
    PartManifest changed = m;
    changed.entries[1].blob_size += 1;
    EXPECT_NE(computePayloadDigest(m), computePayloadDigest(changed));
}

/// No-smuggling: one extra trailing byte after the last payload-zone segment (or after the trailer,
/// when there are no Inline entries) must be rejected - exercises the final `!in.eof()` check.
TEST(CASPartManifestFormat, TrailingByteAfterPayloadZoneFailsClosed)
{
    String bad = encodePartManifest(sample());
    bad += "X";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(bad); });
}

/// An Inline entry's record "il" disagrees with what the payload zone's banner+bytes actually
/// declare (the banner and bytes are left as originally written; only the record line's "il" is
/// edited). The record's declared `il` is what decode uses both to build the expected banner text
/// and to know how many bytes to read from the zone, so this must fail closed rather than silently
/// reading the wrong byte count.
TEST(CASPartManifestFormat, InlineRecordIlMismatchWithPayloadZoneBannerFailsClosed)
{
    String bad = encodePartManifest(sample());
    const String needle = "\"il\":12";
    const size_t pos = bad.find(needle);
    ASSERT_NE(pos, String::npos);
    bad.replace(pos, needle.size(), "\"il\":13");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartManifest(bad); });
}

/// ==== migrated from gtest_cas_manifest_codec.cpp (deleted in the phase-6 binary->text cutover,
/// Task 3): these exercise refMatchesBody/manifestNamespaceMatches/findEntry/entryRange, pure
/// functions carried over verbatim from the retired binary codec (untouched by the wire-shape
/// migration) — reusing this file's own sample() fixture instead of reintroducing a second one. ====

TEST(CASPartManifestFormat, RefMatchesBodyAcceptsExactRef)
{
    const PartManifest m = sample();
    /// The journal ref equals the body ref -> true.
    EXPECT_TRUE(refMatchesBody(m.ref, m));
}

TEST(CASPartManifestFormat, RefMatchesBodyRejectsEachFieldMismatch)
{
    const PartManifest m = sample();
    ManifestRef wrong_writer = m.ref; wrong_writer.writer_epoch = m.ref.writer_epoch + 1;
    ManifestRef wrong_seq = m.ref;    wrong_seq.build_sequence = m.ref.build_sequence + 1;
    ManifestRef wrong_inst = m.ref;   wrong_inst.manifest_ordinal = m.ref.manifest_ordinal + 1;
    EXPECT_FALSE(refMatchesBody(wrong_writer, m));
    EXPECT_FALSE(refMatchesBody(wrong_seq, m));
    EXPECT_FALSE(refMatchesBody(wrong_inst, m));
}

TEST(CASPartManifestFormat, ManifestNamespaceMatchesAcceptsOwningNs)
{
    const PartManifest m = sample();
    EXPECT_TRUE(manifestNamespaceMatches(m.root_namespace_id, m));
}

TEST(CASPartManifestFormat, ManifestNamespaceMatchesRejectsForeignNs)
{
    const PartManifest m = sample();
    /// sample()'s namespace is "00/aa@cas@" — pick a genuinely foreign one and a strict-prefix one.
    EXPECT_FALSE(manifestNamespaceMatches(RootNamespace("00/bb@cas@"), m));
    /// A namespace that is a prefix but not equal is still a mismatch (no loose comparison).
    EXPECT_FALSE(manifestNamespaceMatches(RootNamespace("00/aa"), m));
}

TEST(CASPartManifestFormat, FindEntryBinarySearch)
{
    std::vector<ManifestEntry> entries;
    for (const char * p : {"a.txt", "b/inner.txt", "b/z.txt", "c.txt"})
    {
        ManifestEntry e;
        e.path = p;
        e.placement = EntryPlacement::Inline;
        e.inline_bytes = "v";
        entries.push_back(e);
    }
    EXPECT_NE(findEntry(entries, "a.txt"), nullptr);
    EXPECT_EQ(findEntry(entries, "a.txt")->path, "a.txt");
    EXPECT_NE(findEntry(entries, "c.txt"), nullptr);          /// last element
    EXPECT_EQ(findEntry(entries, "b"), nullptr);              /// prefix of a path, not a path
    EXPECT_EQ(findEntry(entries, "zzz"), nullptr);            /// past the end
    EXPECT_EQ(findEntry({}, "a"), nullptr);                   /// empty
}

TEST(CASPartManifestFormat, EntryRangeContiguousPrefix)
{
    std::vector<ManifestEntry> entries;
    for (const char * p : {"a.txt", "p.proj/data.bin", "p.proj/x.txt", "q.txt"})
    {
        ManifestEntry e;
        e.path = p;
        e.placement = EntryPlacement::Inline;
        e.inline_bytes = "v";
        entries.push_back(e);
    }
    auto [first, last] = entryRange(entries, "p.proj/");
    ASSERT_EQ(last - first, 2);
    EXPECT_EQ(first->path, "p.proj/data.bin");
    EXPECT_EQ((last - 1)->path, "p.proj/x.txt");

    auto [w1, w2] = entryRange(entries, "");                  /// empty prefix = whole span
    EXPECT_EQ(w2 - w1, 4);

    auto [n1, n2] = entryRange(entries, "zzz/");              /// no match
    EXPECT_EQ(n1, n2);
}
