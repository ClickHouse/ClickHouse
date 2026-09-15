#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcOutcomesFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>

#include <magic_enum.hpp>

using namespace DB::Cas;

namespace
{

/// Same tiny inline copy as `gtest_cas_part_manifest_format.cpp`'s `expectThrowsCode`: stays clear
/// of `Disks/tests/cas_test_helpers.h`, which would drag in the whole CAS backend/store machinery
/// this file otherwise has no need for.
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

CAS_BATTERY_COVERS(GcOutcomes);

TEST(CASFormatBattery, GcOutcomes)
{
    OutcomeLog log;
    OutcomeEntry e;
    e.kind = ObjectKind::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("00112233445566778899aabbccddeeff"))};
    e.token = PersistedEtag{"etag", "e-1"};
    e.outcome = OutcomeKind::Deleted;
    log.entries.push_back(e);
    runFormatBattery({FormatId::GcOutcomes,
        [&] { return sealObject(FormatId::GcOutcomes, encodeOutcomeLog(log)); },
        [](std::string_view d) { decodeOutcomeLog(std::string(openObject(FormatId::GcOutcomes, d))); },
        currentFormatHeader("cas_gc_outcomes") +
        "{\"kind\":\"blob\",\"algo\":\"ch128\",\"digest\":\"00112233445566778899aabbccddeeff\","
        "\"token_type\":\"etag\",\"token\":\"e-1\",\"outcome\":\"deleted\"}\n{\"n\":1}\n"});
}

TEST(CASGCOutcomesFormat, EmptyRoundTrips)
{
    EXPECT_EQ(decodeOutcomeLog(encodeOutcomeLog(OutcomeLog{})).entries.size(), 0u);
}

TEST(CASGCOutcomesFormat, MultiEntryRoundTripAllOutcomes)
{
    OutcomeLog log;
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("aa00000000000000000000000000000a"))},
                           PersistedEtag{"etag", "etag-1"}, OutcomeKind::Deleted});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("bb00000000000000000000000000000b"))},
                           PersistedEtag{"emulated", "7"}, OutcomeKind::Spared});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("cc00000000000000000000000000000c"))},
                           PersistedEtag{"emulated", "8"}, OutcomeKind::Replaced});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("dd00000000000000000000000000000d"))},
                           PersistedEtag{"emulated", "9"}, OutcomeKind::Absent});
    const String text = encodeOutcomeLog(log);
    const OutcomeLog d = decodeOutcomeLog(text);
    ASSERT_EQ(d.entries.size(), 4u);
    EXPECT_EQ(d.entries[0].ref, (BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("aa00000000000000000000000000000a"))}));
    EXPECT_EQ(d.entries[0].outcome, OutcomeKind::Deleted);
    EXPECT_EQ(d.entries[1].outcome, OutcomeKind::Spared);
    EXPECT_EQ(d.entries[2].outcome, OutcomeKind::Replaced);
    EXPECT_EQ(d.entries[3].outcome, OutcomeKind::Absent);
    EXPECT_EQ(d.entries[0].token.value, "etag-1");
    EXPECT_EQ(d.entries[0].token.dialect, "etag");
    EXPECT_EQ(d.entries[3].token.value, "9");
    /// Insertion order + byte-stable text (the encoder is a pure function of the log).
    EXPECT_EQ(encodeOutcomeLog(d), text);
}

/// Closed-set pin: the four `OutcomeKind` words, walked through `magic_enum::enum_values`, which is what proves the
/// renderer and the parser consult the SAME table: a table entry missing altogether is already a
/// build error at the coverage assert, but two delegates drifting onto different tables is not.
TEST(CASGCOutcomesFormat, ClosedSetPinsOutcomeKindWords)
{
    EXPECT_EQ(outcomeKindToWireWord(OutcomeKind::Deleted), "deleted");
    EXPECT_EQ(outcomeKindToWireWord(OutcomeKind::Absent), "absent");
    EXPECT_EQ(outcomeKindToWireWord(OutcomeKind::Replaced), "replaced");
    EXPECT_EQ(outcomeKindToWireWord(OutcomeKind::Spared), "spared");
    for (const auto o : magic_enum::enum_values<OutcomeKind>())
        EXPECT_EQ(outcomeKindFromWireWord(outcomeKindToWireWord(o)), o);
}

TEST(CASGCOutcomesFormat, RecordRequiresCompleteBlobRefAndTokenGroups)
{
    OutcomeLog log;
    log.entries.push_back({ObjectKind::Blob,
        BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("00112233445566778899aabbccddeeff"))},
        PersistedEtag{"etag", "e-1"}, OutcomeKind::Deleted});
    const String bytes = encodeOutcomeLog(log);

    for (const auto & [field, expected_message] : {
        std::pair{String(R"(,"algo":"ch128")"), "CAS outcome log: blob ref missing algo/digest"},
        std::pair{String(R"(,"digest":"00112233445566778899aabbccddeeff")"), "CAS outcome log: blob ref missing algo/digest"},
        std::pair{String(R"(,"token_type":"etag")"), "CAS outcome log: token missing token_type/token"},
        std::pair{String(R"(,"token":"e-1")"), "CAS outcome log: token missing token_type/token"},
    })
    {
        const auto pos = bytes.find(field);
        ASSERT_NE(pos, String::npos);
        String incomplete = bytes;
        incomplete.erase(pos, field.size());
        try
        {
            decodeOutcomeLog(incomplete);
            FAIL() << "expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
            EXPECT_EQ(e.message(), expected_message);
        }
    }
}

TEST(CASGCOutcomesFormat, GarbageAndUnknownWordsFailClosed)
{
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { decodeOutcomeLog(String("")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [] { decodeOutcomeLog(String("not a cas object\n")); });
    /// A record with an unknown outcome word fails closed.
    const String bad = "{\"type\":\"cas_gc_outcomes\",\"v\":1}\n"
                       "{\"kind\":\"blob\",\"algo\":\"ch128\",\"digest\":\"00112233445566778899aabbccddeeff\","
                       "\"token_type\":\"etag\",\"token\":\"x\",\"outcome\":\"bogus\"}\n{\"n\":1}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeOutcomeLog(bad); });
    /// A trailer count mismatch fails closed.
    const String miscount = "{\"type\":\"cas_gc_outcomes\",\"v\":1}\n{\"n\":5}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeOutcomeLog(miscount); });
}

TEST(CASGCOutcomesFormat, DigestWidthMismatchFailsClosedWithCorruptedData)
{
    /// `ch128` (CityHash128) digests are 16 bytes = 32 hex chars; here the `digest` field is truncated
    /// to 30 hex chars. Must surface as CORRUPTED_DATA (malformed serialized input), not
    /// `fromHex`'s BAD_ARGUMENTS.
    const String bad = "{\"type\":\"cas_gc_outcomes\",\"v\":1}\n"
                       "{\"kind\":\"blob\",\"algo\":\"ch128\",\"digest\":\"00112233445566778899aabbccddee\","
                       "\"token_type\":\"etag\",\"token\":\"x\",\"outcome\":\"deleted\"}\n{\"n\":1}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeOutcomeLog(bad); });
}
