#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcOutcomesFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

#include <magic_enum.hpp>
#include <type_traits>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; }

namespace
{
/// Same tiny inline copy as `gtest_cas_part_manifest_format.cpp`'s `expectThrowsCode`: stays clear
/// of `Disks/tests/cas_test_helpers.h`'s `DB::Cas::tests::expectThrowsCode`, which would both drag
/// in the whole CAS backend/store machinery this file otherwise has no need for AND collide (same
/// namespace, same name and signature) if that header were ever included here too.
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
}

static_assert(DB::Cas::casEnumTableCoversEnum<DB::Cas::kTokenTypeWords, DB::Cas::Dialect>());
static_assert(DB::Cas::casEnumTableCoversEnum<DB::Cas::kObjectKindWords, DB::Cas::ObjectKind>());
static_assert(DB::Cas::casEnumTableCoversEnum<DB::Cas::kBlobHashAlgoWords, DB::Cas::BlobHashAlgo>());

TEST(CASWireVocab, EnumTablesPinTheCurrentWords)
{
    using namespace DB::Cas;
    EXPECT_EQ(kTokenTypeWords.toWord(Dialect::ETag, "t"), "etag");
    EXPECT_EQ(kTokenTypeWords.toWord(Dialect::Generation, "t"), "generation");
    EXPECT_EQ(kTokenTypeWords.toWord(Dialect::Emulated, "t"), "emulated");
    EXPECT_EQ(kBlobHashAlgoWords.toWord(BlobHashAlgo::CityHash128, "t"), "ch128");
    EXPECT_EQ(kBlobHashAlgoWords.toWord(BlobHashAlgo::XXH3_128, "t"), "xxh3");
    EXPECT_EQ(kBlobHashAlgoWords.toWord(BlobHashAlgo::Sha256, "t"), "sha256");
    EXPECT_EQ(kObjectKindWords.toWord(ObjectKind::Blob, "t"), "blob");
    EXPECT_EQ(refOwnerKindToWord(RefOwnerKind::Committed), "committed");
    EXPECT_EQ(refOwnerKindToWord(RefOwnerKind::Precommit), "precommit");
}

/// Every enum wire table's closed set, walked through `magic_enum::enum_values` rather than a
/// hand-copied list -- a future enumerator the encoder can construct but no table entry covers
/// would otherwise round-trip silently through the untested value.
TEST(CASWireVocab, ClosedSetsRoundTripEveryEnumeratorExhaustively)
{
    for (const auto t : magic_enum::enum_values<Dialect>())
        EXPECT_EQ(kTokenTypeWords.fromWord(kTokenTypeWords.toWord(t, "t"), "t"), t);
    for (const auto k : magic_enum::enum_values<ObjectKind>())
        EXPECT_EQ(objectKindFromWord(objectKindToWord(k), "k"), k);
    for (const auto a : magic_enum::enum_values<BlobHashAlgo>())
        EXPECT_EQ(blobHashAlgoFromWord(blobHashAlgoName(a), "a"), a);
    for (const auto k : magic_enum::enum_values<RefOwnerKind>())
        EXPECT_EQ(refOwnerKindFromWord(refOwnerKindToWord(k), "k"), k);
}

TEST(CASWireVocab, EnumWordsRoundTrip)
{
    for (Dialect t : {Dialect::ETag, Dialect::Generation, Dialect::Emulated})
        EXPECT_EQ(kTokenTypeWords.fromWord(kTokenTypeWords.toWord(t, "t"), "t"), t);
    for (BlobHashAlgo a : {BlobHashAlgo::CityHash128, BlobHashAlgo::XXH3_128, BlobHashAlgo::Sha256})
        EXPECT_EQ(blobHashAlgoFromWord(blobHashAlgoName(a), "a"), a);
    EXPECT_EQ(objectKindFromWord(objectKindToWord(ObjectKind::Blob), "k"), ObjectKind::Blob);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { dialectWordFromString("nope", "t"); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { blobHashAlgoFromWord("nope", "a"); });
}

TEST(CASWireVocab, SiblingFieldsWriteAndReadBack)
{
    CasJsonWriter out;
    bool first = true;
    writeTokenFields(out, first, PersistedEtag{"etag", "etag-abc\"x"});
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("00112233445566778899aabbccddeeff"))};
    writeBlobRefFields(out, first, ref);
    closeObject(out, first);
    const String rendered = std::move(out).take();
    EXPECT_EQ(rendered,
        R"({"token_type":"etag","token":"etag-abc\"x","algo":"ch128","digest":"00112233445566778899aabbccddeeff"})");

    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    String key;
    String tv;
    String ha;
    String h;
    String tt;
    while (r.nextKey(key))
    {
        if (key == "token_type") tt = String(dialectWordFromString(r.readString(), "t"));
        else if (key == "token") tv = r.readString();
        else if (key == "algo") ha = r.readString();
        else if (key == "digest") h = r.readString();
        else r.skipUnknown(key);
    }
    EXPECT_EQ(tt, "etag");
    EXPECT_EQ(tv, "etag-abc\"x");
    const BlobRef back{blobHashAlgoFromWord(ha, "a"), codecFor(blobHashAlgoFromWord(ha, "a")).fromHex(h)};
    EXPECT_EQ(back, ref);
}

TEST(CASWireVocab, ManifestRefBundleWritesTheOldPrefixedKeys)
{
    using namespace DB::Cas;
    CasJsonWriter w;
    bool first = true;
    writeManifestRefFields(w, first, kOldManifestRefKeys, ManifestRef{1, 2, 3});
    w.closeObject(first);
    EXPECT_EQ(std::move(w).take(), R"({"old_epoch":"1","old_build":"2","old_ord":3})");
}

TEST(CASWireVocab, MatchAndBuildRoundTripsABlobRef)
{
    using namespace DB::Cas;
    const String rendered = R"({"algo":"ch128","digest":"00112233445566778899aabbccddeeff"})";
    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    BlobRefFields fields;
    String key;
    while (r.nextKey(key))
    {
        if (matchBlobRefFields(key, r, fields))
            continue;
        r.skipUnknown(key);
    }
    const BlobRef ref = fields.build("t");
    EXPECT_EQ(kBlobHashAlgoWords.toWord(ref.algo, "t"), "ch128");
}

TEST(CASWireVocab, BlobRefBuildFailsClosedOnHalfAGroupAndOnBadWidth)
{
    using namespace DB::Cas;
    BlobRefFields only_algo;
    only_algo.algo_word = "ch128";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { only_algo.build("t"); });

    BlobRefFields short_digest;
    short_digest.algo_word = "ch128";
    short_digest.digest_hex = "00112233445566778899aabbccddee";   /// 30 hex chars, needs 32
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { short_digest.build("t"); });
}

TEST(CASWireVocab, BlobRefBuildFailsClosedOnRightWidthNonHexDigest)
{
    using namespace DB::Cas;
    BlobRefFields bad_hex;
    bad_hex.algo_word = "ch128";
    bad_hex.digest_hex = "gg112233445566778899aabbccddeeff";   /// 32 chars (right width), not hex
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { bad_hex.build("t"); });
}

TEST(CASWireVocab, MatchManifestRefFieldsAndBuildRefRoundTripInAnyKeyOrder)
{
    using namespace DB::Cas;
    /// Fed out of writer order (ord, epoch, build) to pin key-order independence. `epoch`/`build` are quoted
    /// decimal strings and `ord` is a bare number -- a swapped read primitive between the two shapes
    /// would fail to parse this literal.
    const String rendered = R"({"ord":3,"epoch":"7","build":"9"})";
    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    ManifestRefFields fields;
    String key;
    while (r.nextKey(key))
    {
        if (matchManifestRefFields(key, r, kBareManifestRefKeys, fields))
            continue;
        r.skipUnknown(key);
    }
    EXPECT_EQ(fields.buildRef("t", "ctx"), (ManifestRef{7, 9, 3}));
}

TEST(CASWireVocab, ManifestRefFieldsBuildRefFailsClosedOnHalfAGroup)
{
    using namespace DB::Cas;
    ManifestRefFields fields;
    fields.epoch = 7;
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { fields.buildRef("t", "ctx"); });
}

TEST(CASWireVocab, MatchTokenFieldsConsumesSemanticKeysAndLeavesUnrelatedKeyUnmatched)
{
    using namespace DB::Cas;
    const String rendered = R"({"token_type":"etag","token":"abc","zz":1})";
    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    TokenFields fields;
    String key;
    bool saw_unmatched = false;
    while (r.nextKey(key))
    {
        if (matchTokenFields(key, r, fields))
            continue;
        saw_unmatched = true;
        r.skipUnknown(key);
    }
    ASSERT_TRUE(fields.type_word.has_value());
    EXPECT_EQ(*fields.type_word, "etag");
    ASSERT_TRUE(fields.value.has_value());
    EXPECT_EQ(*fields.value, "abc");
    EXPECT_TRUE(saw_unmatched);
}

TEST(CASWireVocab, TokenFieldsBuildsInAnyKeyOrderAndRequiresBothFields)
{
    const String rendered = R"({"token":"abc","token_type":"etag"})";
    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    TokenFields fields;
    String key;
    while (r.nextKey(key))
    {
        if (matchTokenFields(key, r, fields))
            continue;
        r.skipUnknown(key);
    }
    const PersistedEtag built = fields.build("t");
    EXPECT_EQ(built.dialect, "etag");
    EXPECT_EQ(built.value, "abc");

    TokenFields only_type;
    only_type.type_word = "etag";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { only_type.build("t"); });
}

TEST(CASWireVocab, OldManifestEpochKeyDoesNotAliasTheSemanticKey)
{
    const String rendered = R"({"me":"1","build":"2","ord":3})";
    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "t");
    ManifestRefFields fields;
    String key;
    while (r.nextKey(key))
    {
        if (matchManifestRefFields(key, r, kBareManifestRefKeys, fields))
            continue;
        r.skipUnknown(key);
    }

    try
    {
        fields.buildRef("RefTableSnapshot", "committed");
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_EQ(e.message(), "CAS RefTableSnapshot: committed manifest_ref missing epoch/build/ord");
    }
}

/// A `PersistedEtag` survives every encoding a durable CAS record uses for one, and the type
/// system refuses the reverse direction: a persisted value must never be trusted to mint a live
/// `Etag`, which only an admitted request may produce.
static_assert(!std::is_constructible_v<DB::Cas::Etag, DB::Cas::PersistedEtag>);

TEST(CASPersistedEtag, RoundTripsThroughEveryFormatAndNeverBecomesAnIncarnation)
{
    const PersistedEtag recorded{"generation", R"(17"3)"};   /// a quote the JSON encodings must escape

    /// 1. The shared `token_type`/`token` JSON pair.
    {
        CasJsonWriter out;
        bool first = true;
        writeTokenFields(out, first, recorded);
        closeObject(out, first);
        const String rendered = std::move(out).take();
        DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
        JsonObjectReader r(in, KeyStrictness::Strict, "t");
        TokenFields fields;
        String key;
        while (r.nextKey(key))
            ASSERT_TRUE(matchTokenFields(key, r, fields)) << "unexpected key " << key;
        const PersistedEtag back = fields.build("t");
        EXPECT_EQ(back.dialect, recorded.dialect);
        EXPECT_EQ(back.value, recorded.value);
    }

    /// 2. The `cas_run` condemned row's NDJSON form.
    {
        const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(9))};
        DB::WriteBufferFromOwnString out;
        SourceEdgeRunWriter writer(out);
        writer.append(SourceEdgeRecord{.ref = ref, .source_id = UInt128(0), .marker = RunMarker::Condemned,
                                       .delete_pending = true, .token = recorded, .size = 64,
                                       .condemn_round = 3, .marker_confirmed = true});
        writer.finish();
        out.finalize();
        const String bytes = out.str();
        DB::ReadBufferFromMemory in(bytes.data(), bytes.size());
        SourceEdgeRunReader reader(in);
        SourceEdgeRecord back;
        ASSERT_TRUE(reader.next(back));
        EXPECT_EQ(back.token.dialect, recorded.dialect);
        EXPECT_EQ(back.token.value, recorded.value);
        EXPECT_FALSE(reader.next(back));
    }

    /// 3. The GC outcome log.
    {
        OutcomeLog log;
        log.entries.push_back(OutcomeEntry{ObjectKind::Blob,
            BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(4))}, recorded, OutcomeKind::Deleted});
        const OutcomeLog back = decodeOutcomeLog(encodeOutcomeLog(log));
        ASSERT_EQ(back.entries.size(), 1u);
        EXPECT_EQ(back.entries[0].token.dialect, recorded.dialect);
        EXPECT_EQ(back.entries[0].token.value, recorded.value);
    }

    /// 4. The condemned row's packed byte form, whose dialect rides one byte rather than a word.
    {
        const CondemnedRow row{.delete_pending = false, .token = recorded, .size = 5,
                               .condemn_round = 11, .marker_confirmed = true};
        EXPECT_EQ(decodeCondemnedRow(encodeCondemnedRow(row)), row);
    }
}

/// Both directions of the dialect vocabulary fail closed, so neither encoding can carry a value the
/// other cannot name.
TEST(CASPersistedEtag, UnknownDialectWordAndByteAreBothRefused)
{
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { dialectWordFromString("etags", "t"); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { dialectByteFromWord("etags", "t"); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { dialectWordFromByte(0, "t"); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { dialectWordFromByte(4, "t"); });
    EXPECT_EQ(dialectWordFromByte(dialectByteFromWord("generation", "t"), "t"), "generation");
}
