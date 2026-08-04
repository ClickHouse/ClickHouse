#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <limits>
#include <algorithm>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; extern const int LOGICAL_ERROR; }

namespace
{
CasFoldSeal sampleFoldSeal()
{
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    seal.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{3, 4}};
    seal.ref_lives[UInt128{2}].coverage = RefCoverage{.classification = 1};
    seal.blob_target_runs.push_back(RunRef{.key = "gc/gen/7/blob_target/0/0", .checksum = UInt128(0xABCDEF)});
    return seal;
}

void eraseRequiredField(String & encoded, std::string_view field)
{
    const size_t pos = encoded.find(field);
    ASSERT_NE(pos, String::npos);
    encoded.erase(pos, field.size());
}
}

TEST(CASFormatBattery, FoldSeal)
{
    CasFoldSeal seal;
    seal.generation = 5;
    seal.parent_generation = 4;
    seal.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{7, 11}};
    seal.blob_target_runs.push_back(RunRef{.key = "r0", .checksum = UInt128(0x0f), .shard = 0, .generation = 5});
    seal.condemned_summary[0] = CondemnedSummary{.condemned_total = 3, .pending_total = 1,
                                                 .oldest_nonpending_condemn_round = 4};
    runFormatBattery({FormatId::FoldSeal,
        [&] { return sealObject(FormatId::FoldSeal, encodeFoldSeal(seal)); },
        [](std::string_view s) { decodeFoldSeal(std::string(openObject(FormatId::FoldSeal, s))); },
        currentFormatHeader("cas_fold_seal") +
        "{\"g\":\"5\",\"pg\":\"4\"}\n"
        "{\"k\":\"rfl\",\"life\":\"00000000000000000000000000000001\",\"cls\":2,\"lfe\":\"7\",\"lfs\":\"11\"}\n"
        "{\"k\":\"btr\",\"key\":\"r0\",\"ck\":\"0000000000000000000000000000000f\",\"shard\":0,\"gen\":\"5\"}\n"
        "{\"k\":\"cnd\",\"shard\":0,\"ct\":3,\"pt\":1,\"ocr\":\"4\"}\n"
        "{\"n\":3}\n"});
}

TEST(CASFoldSealFormat, RoundTripsAllFields)
{
    const CasFoldSeal in = sampleFoldSeal();
    const CasFoldSeal out = decodeFoldSeal(encodeFoldSeal(in));

    EXPECT_EQ(out.generation, in.generation);
    EXPECT_EQ(out.parent_generation, in.parent_generation);
    ASSERT_EQ(out.ref_lives.size(), in.ref_lives.size());
    EXPECT_EQ(out.ref_lives.at(UInt128{1}).coverage.classification, 2);
    EXPECT_EQ(out.ref_lives.at(UInt128{1}).coverage.last_folded_ref_id, (RefTxnId{3, 4}));
    ASSERT_EQ(out.blob_target_runs.size(), 1u);
    EXPECT_EQ(out.blob_target_runs[0].key, "gc/gen/7/blob_target/0/0");
    EXPECT_EQ(out.blob_target_runs[0].checksum, UInt128(0xABCDEF));
    EXPECT_EQ(out, in);
}

TEST(CASFoldSealFormat, AuthoritativeDecodeRejectsTwoBlobTargetRunsForOneShard)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    seal.blob_target_runs = {
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .generation = 7},
    };
    seal.condemned_summary[0] = CondemnedSummary{};

    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, /*gc_shards=*/1); },
        "duplicate blob-target shard");
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASFoldSealFormatDeathTest, ProducerValidationRejectsMalformedSealBeforePut)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.blob_target_runs = {
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .generation = 7}};
    seal.condemned_summary[0] = CondemnedSummary{};
    EXPECT_DEATH({ validateFoldSealForWrite(seal, layout, 1); }, "duplicate blob-target shard");
}
#else
TEST(CASFoldSealFormat, ProducerValidationRejectsMalformedSealBeforePut)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.blob_target_runs = {
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .generation = 7}};
    seal.condemned_summary[0] = CondemnedSummary{};
    cas_battery_detail::expectCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { validateFoldSealForWrite(seal, layout, 1); }, "duplicate blob-target shard");
}
#endif

TEST(CASFoldSealFormat, AuthoritativeDecodeRequiresEveryBlobTargetAndSummaryField)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    seal.blob_target_runs.push_back(RunRef{
        .key = layout.blobTargetRunKey(7, 1, 0, 0),
        .checksum = UInt128{1},
        .shard = 0,
        .generation = 7});
    seal.condemned_summary[0] = CondemnedSummary{};
    const String valid = encodeFoldSeal(seal);

    for (const std::string_view field : {
        R"(,"key":"p/gc/gen/7/attempt/1/blob_target/0/0")",
        R"(,"ck":"00000000000000000000000000000001")",
        R"(,"gen":"7")",
        ",\"ct\":0",
        ",\"pt\":0",
        R"(,"ocr":"18446744073709551615")"})
    {
        String malformed = valid;
        eraseRequiredField(malformed, field);
        cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
            [&] { decodeFoldSeal(malformed, layout, 1); }, "missing");
    }

    /// `shard` occurs once on each row; remove each occurrence independently.
    String missing_btr_shard = valid;
    eraseRequiredField(missing_btr_shard, ",\"shard\":0");
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(missing_btr_shard, layout, 1); }, "missing");

    String missing_cnd_shard = valid;
    const size_t first_shard = missing_cnd_shard.find(",\"shard\":0");
    ASSERT_NE(first_shard, String::npos);
    const size_t second_shard = missing_cnd_shard.find(",\"shard\":0", first_shard + 1);
    ASSERT_NE(second_shard, String::npos);
    missing_cnd_shard.erase(second_shard, std::string_view(",\"shard\":0").size());
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(missing_cnd_shard, layout, 1); }, "missing");
}

TEST(CASFoldSealFormat, AuthoritativeDecodeRejectsNoncanonicalRowsAndIncompleteSummaryDomain)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    seal.blob_target_runs.push_back(RunRef{
        .key = layout.blobTargetRunKey(7, 1, 1, 0),
        .checksum = UInt128{1},
        .shard = 1,
        .generation = 7});
    seal.condemned_summary[0] = CondemnedSummary{};

    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "outside");

    seal.blob_target_runs[0].shard = 0;
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "not canonical");

    seal.blob_target_runs.clear();
    seal.condemned_summary.clear();
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "exactly 1");

    seal.condemned_summary[0] = CondemnedSummary{};
    seal.condemned_summary[1] = CondemnedSummary{};
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "exactly 1");
}

TEST(CASFoldSealFormat, AuthoritativeDecodeRejectsContradictorySummaryCounts)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.condemned_summary[0] = CondemnedSummary{
        .condemned_total = 1,
        .pending_total = 2,
        .oldest_nonpending_condemn_round = 3};
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "greater than");

    seal.condemned_summary[0] = CondemnedSummary{
        .condemned_total = 2,
        .pending_total = 1,
        .oldest_nonpending_condemn_round = std::numeric_limits<uint64_t>::max()};
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encodeFoldSeal(seal), layout, 1); }, "real oldest");
}

TEST(CASFoldSealFormat, RejectsUnexpectedGeneration)
{
    CasFoldSeal seal;
    seal.generation = 5;
    const String encoded = encodeFoldSeal(seal);

    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(encoded, /*expected_generation=*/6); }, "unexpected generation");
    EXPECT_EQ(decodeFoldSeal(encoded, /*expected_generation=*/5).generation, 5);
    EXPECT_EQ(decodeFoldSeal(encoded).generation, 5);
}

TEST(CASFoldSeal, EncodingIsByteDeterministic)
{
    const CasFoldSeal in = sampleFoldSeal();
    EXPECT_EQ(encodeFoldSeal(in), encodeFoldSeal(in));
}

TEST(CASFoldSealFormat, TextIsByteDeterministic)
{
    CasFoldSeal a;
    a.generation = 5;
    a.parent_generation = 4;
    a.blob_target_runs = {RunRef{"z", UInt128(2), 1, 5}, RunRef{"a", UInt128(1), 0, 5}};
    CasFoldSeal b = a;
    std::reverse(b.blob_target_runs.begin(), b.blob_target_runs.end());   /// same set, different order
    EXPECT_EQ(encodeFoldSeal(a), encodeFoldSeal(b));   /// encoder must sort runs by key
}

TEST(CASFoldSeal, RejectsEmptyAndBadMagic)
{
    EXPECT_ANY_THROW(decodeFoldSeal(""));
    EXPECT_ANY_THROW(decodeFoldSeal("not-a-seal"));
}

TEST(CASFoldSeal, CoverageRecordsEveryCatalogLife)
{
    CasFoldSeal in = sampleFoldSeal();
    in.ref_lives[UInt128{3}].coverage = RefCoverage{.classification = 0};
    const CasFoldSeal out = decodeFoldSeal(encodeFoldSeal(in));
    EXPECT_TRUE(out.ref_lives.contains(UInt128{3}));
    EXPECT_EQ(out.ref_lives.size(), 3u);
}

TEST(CASFoldSeal, FoldSealCondemnedSummaryRoundTrips)
{
    /// A seal carrying a non-empty condemned_summary over 2 shards (one a zero entry) round-trips and
    /// compares equal, and the UINT64_MAX "none" sentinel survives.
    CasFoldSeal s;
    s.generation = 9;
    s.parent_generation = 8;
    s.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = 2};
    s.blob_target_runs.push_back(RunRef{.key = "gc/gen/9/blob_target/0/0", .checksum = UInt128(0x77),
                                        .shard = 0, .generation = 9});
    s.condemned_summary[0] = CondemnedSummary{.condemned_total = 3, .pending_total = 1,
                                              .oldest_nonpending_condemn_round = 5};
    s.condemned_summary[1] = CondemnedSummary{};   /// explicit zero entry (totality over gc_shards)

    const CasFoldSeal out = decodeFoldSeal(encodeFoldSeal(s));
    EXPECT_EQ(out, s);
    ASSERT_EQ(out.condemned_summary.size(), 2u);
    EXPECT_EQ(out.condemned_summary.at(0).condemned_total, 3u);
    EXPECT_EQ(out.condemned_summary.at(0).pending_total, 1u);
    EXPECT_EQ(out.condemned_summary.at(0).oldest_nonpending_condemn_round, 5u);
    EXPECT_EQ(out.condemned_summary.at(1).oldest_nonpending_condemn_round,
              std::numeric_limits<uint64_t>::max());   /// UINT64_MAX sentinel survives

    EXPECT_TRUE(decodeFoldSeal(encodeFoldSeal(CasFoldSeal{})).condemned_summary.empty());
}

/// Mutation caught: restoring separate `cov` and `nsc` rows, dropping the cleanup evidence, or
/// serializing the row under a logical namespace changes these literal generation-8 bytes.
TEST(CASFoldSealFormat, UnifiedRefLifeRowRoundTripsCoverageHoldAndCleanupEvidence)
{
    CasFoldSeal seal;
    seal.generation = 8;
    seal.parent_generation = 7;
    const UInt128 life_id{0x1234};
    seal.ref_lives.emplace(life_id, RefLifeFoldState{
        .coverage = RefCoverage{
            .classification = 4,
            .last_folded_ref_id = RefTxnId{3, 4},
            .hold = RefHold{
                .reason = HoldReason::ManifestBodyMissing,
                .offending_position = RefTxnId{5, 6},
                .retry_count = 7,
                .next_retry_round = 8}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{9, 10}}});

    const String expected = currentFormatHeader("cas_fold_seal") +
        "{\"g\":\"8\",\"pg\":\"7\"}\n"
        "{\"k\":\"rfl\",\"life\":\"00000000000000000000000000001234\",\"cls\":4,"
        "\"lfe\":\"3\",\"lfs\":\"4\",\"hr\":\"manifest_body_missing\",\"hpe\":\"5\","
        "\"hps\":\"6\",\"hrc\":7,\"hnr\":\"8\",\"rte\":\"9\",\"rts\":\"10\"}\n"
        "{\"n\":1}\n";

    EXPECT_EQ(encodeFoldSeal(seal), expected);
    EXPECT_EQ(decodeFoldSeal(expected), seal);
}

/// Mutation caught: accepting the generation-6 split coverage collection would leave a second
/// namespace-keyed source of lifecycle work in a generation-7 process.
TEST(CASFoldSealFormat, UnifiedCodecRejectsLegacyCoverageRecord)
{
    const String old =
        "{\"type\":\"cas_fold_seal\",\"v\":7}\n"
        "{\"g\":\"8\",\"pg\":\"7\"}\n"
        "{\"k\":\"cov\",\"key\":\"name/0\",\"cls\":2,\"lfe\":\"3\",\"lfs\":\"4\"}\n"
        "{\"n\":1}\n";
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(old); }, "legacy coverage");
}

/// Mutation caught: accepting the generation-6 cleanup-item state would restore the independent
/// marker-driven `Pending`/`Completed` handshake.
TEST(CASFoldSealFormat, UnifiedCodecRejectsLegacyNamespaceCleanupRecord)
{
    const String old =
        "{\"type\":\"cas_fold_seal\",\"v\":7}\n"
        "{\"g\":\"8\",\"pg\":\"7\"}\n"
        "{\"k\":\"nsc\",\"ns\":\"name\",\"rte\":\"3\",\"rts\":\"4\",\"st\":\"completed\"}\n"
        "{\"n\":1}\n";
    cas_battery_detail::expectCode(
        DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(old); }, "legacy namespace cleanup");
}
