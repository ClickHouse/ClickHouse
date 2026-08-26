#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcOutcomesFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>

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

TEST(CASFormatBattery, GcOutcomes)
{
    OutcomeLog log;
    OutcomeEntry e;
    e.kind = ObjectKind::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("00112233445566778899aabbccddeeff"))};
    e.token = Token{"e-1", TokenType::ETag};
    e.outcome = OutcomeKind::Deleted;
    log.entries.push_back(e);
    runFormatBattery({FormatId::GcOutcomes,
        [&] { return sealObject(FormatId::GcOutcomes, encodeOutcomeLog(log)); },
        [](std::string_view d) { decodeOutcomeLog(std::string(openObject(FormatId::GcOutcomes, d))); },
        currentFormatHeader("cas_gc_outcomes") +
        "{\"k\":\"blob\",\"ha\":\"ch128\",\"h\":\"00112233445566778899aabbccddeeff\","
        "\"tt\":\"etag\",\"tv\":\"e-1\",\"oc\":\"deleted\"}\n{\"n\":1}\n"});
}

TEST(CASGCOutcomesFormat, EmptyRoundTrips)
{
    EXPECT_EQ(decodeOutcomeLog(encodeOutcomeLog(OutcomeLog{})).entries.size(), 0u);
}

TEST(CASGCOutcomesFormat, MultiEntryRoundTripAllOutcomes)
{
    OutcomeLog log;
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("aa00000000000000000000000000000a"))},
                           Token{"etag-1", TokenType::ETag}, OutcomeKind::Deleted});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("bb00000000000000000000000000000b"))},
                           Token{"7", TokenType::Emulated}, OutcomeKind::Spared});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("cc00000000000000000000000000000c"))},
                           Token{"8", TokenType::Emulated}, OutcomeKind::Replaced});
    log.entries.push_back({ObjectKind::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("dd00000000000000000000000000000d"))},
                           Token{"9", TokenType::Emulated}, OutcomeKind::Absent});
    const String text = encodeOutcomeLog(log);
    const OutcomeLog d = decodeOutcomeLog(text);
    ASSERT_EQ(d.entries.size(), 4u);
    EXPECT_EQ(d.entries[0].ref, (BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128("aa00000000000000000000000000000a"))}));
    EXPECT_EQ(d.entries[0].outcome, OutcomeKind::Deleted);
    EXPECT_EQ(d.entries[1].outcome, OutcomeKind::Spared);
    EXPECT_EQ(d.entries[2].outcome, OutcomeKind::Replaced);
    EXPECT_EQ(d.entries[3].outcome, OutcomeKind::Absent);
    EXPECT_EQ(d.entries[0].token.value, "etag-1");
    EXPECT_EQ(d.entries[0].token.type, TokenType::ETag);
    EXPECT_EQ(d.entries[3].token.value, "9");
    /// Insertion order + byte-stable text (the encoder is a pure function of the log).
    EXPECT_EQ(encodeOutcomeLog(d), text);
}

TEST(CASGCOutcomesFormat, GarbageAndUnknownWordsFailClosed)
{
    EXPECT_THROW(decodeOutcomeLog(String("")), DB::Exception);
    EXPECT_THROW(decodeOutcomeLog(String("not a cas object\n")), DB::Exception);
    /// A record with an unknown outcome word fails closed.
    const String bad = "{\"type\":\"cas_gc_outcomes\",\"v\":3}\n"
                       "{\"k\":\"blob\",\"ha\":\"ch128\",\"h\":\"00112233445566778899aabbccddeeff\","
                       "\"tt\":\"etag\",\"tv\":\"x\",\"oc\":\"bogus\"}\n{\"n\":1}\n";
    EXPECT_THROW(decodeOutcomeLog(bad), DB::Exception);
    /// A trailer count mismatch fails closed.
    const String miscount = "{\"type\":\"cas_gc_outcomes\",\"v\":3}\n{\"n\":5}\n";
    EXPECT_THROW(decodeOutcomeLog(miscount), DB::Exception);
}

TEST(CASGCOutcomesFormat, DigestWidthMismatchFailsClosedWithCorruptedData)
{
    /// `ch128` (CityHash128) digests are 16 bytes = 32 hex chars; here the "h" field is truncated
    /// to 30 hex chars. Must surface as CORRUPTED_DATA (malformed serialized input), not
    /// `fromHex`'s BAD_ARGUMENTS.
    const String bad = "{\"type\":\"cas_gc_outcomes\",\"v\":3}\n"
                       "{\"k\":\"blob\",\"ha\":\"ch128\",\"h\":\"00112233445566778899aabbccddee\","
                       "\"tt\":\"etag\",\"tv\":\"x\",\"oc\":\"deleted\"}\n{\"n\":1}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeOutcomeLog(bad); });
}
