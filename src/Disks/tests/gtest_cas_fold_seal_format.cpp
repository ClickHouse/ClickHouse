#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <limits>
#include <algorithm>

#include <magic_enum.hpp>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; extern const int LOGICAL_ERROR; }

namespace
{
CasFoldSeal sampleFoldSeal()
{
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    seal.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = CoverageClass::Folded, .last_folded_ref_id = RefTxnId{3, 4}};
    seal.ref_lives[UInt128{2}].coverage = RefCoverage{.classification = CoverageClass::Unchanged};
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

CAS_BATTERY_COVERS(FoldSeal);

TEST(CASFormatBattery, FoldSeal)
{
    CasFoldSeal seal;
    seal.generation = 5;
    seal.parent_generation = 4;
    seal.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = CoverageClass::Folded, .last_folded_ref_id = RefTxnId{7, 11}};
    seal.blob_target_runs.push_back(RunRef{.key = "r0", .checksum = UInt128(0x0f), .shard = 0, .key_generation = 5});
    seal.condemned_summary[0] = CondemnedSummary{.condemned_total = 3, .pending_total = 1,
                                                 .oldest_nonpending_condemn_round = 4};
    runFormatBattery({FormatId::FoldSeal,
        [&] { return sealObject(FormatId::FoldSeal, encodeFoldSeal(seal)); },
        [](std::string_view s) { decodeFoldSeal(std::string(openObject(FormatId::FoldSeal, s))); },
        currentFormatHeader("cas_fold_seal") +
        "{\"generation\":\"5\",\"parent_generation\":\"4\"}\n"
        "{\"kind\":\"ref_life\",\"life\":\"00000000000000000000000000000001\",\"class\":\"folded\",\"fold_epoch\":\"7\",\"fold_seq\":\"11\"}\n"
        "{\"kind\":\"blob_run\",\"key\":\"r0\",\"checksum\":\"0000000000000000000000000000000f\",\"shard\":0,\"key_generation\":\"5\"}\n"
        "{\"kind\":\"condemned\",\"shard\":0,\"condemned\":3,\"pending\":1,\"oldest_round\":\"4\"}\n"
        "{\"n\":3}\n"});
}

TEST(CASFoldSealFormat, RoundTripsAllFields)
{
    const CasFoldSeal in = sampleFoldSeal();
    const CasFoldSeal out = decodeFoldSeal(encodeFoldSeal(in));

    EXPECT_EQ(out.generation, in.generation);
    EXPECT_EQ(out.parent_generation, in.parent_generation);
    ASSERT_EQ(out.ref_lives.size(), in.ref_lives.size());
    EXPECT_EQ(out.ref_lives.at(UInt128{1}).coverage.classification, CoverageClass::Folded);
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
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .key_generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .key_generation = 7},
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
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .key_generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .key_generation = 7}};
    seal.condemned_summary[0] = CondemnedSummary{};
    EXPECT_DEATH({ validateFoldSealForWrite(seal, layout, 1); }, "duplicate blob-target shard");
}
#else
TEST(CASFoldSealFormat, ProducerValidationRejectsMalformedSealBeforePut)
{
    const Layout layout("p");
    CasFoldSeal seal;
    seal.blob_target_runs = {
        RunRef{.key = layout.blobTargetRunKey(7, 1, 0, 0), .checksum = UInt128{1}, .shard = 0, .key_generation = 7},
        RunRef{.key = layout.blobTargetRunKey(7, 2, 0, 0), .checksum = UInt128{2}, .shard = 0, .key_generation = 7}};
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
        .key_generation = 7});
    seal.condemned_summary[0] = CondemnedSummary{};
    const String valid = encodeFoldSeal(seal);

    for (const std::string_view field : {
        R"(,"key":"p/gc/gen/7/attempt/1/blob_target/0/0")",
        R"(,"checksum":"00000000000000000000000000000001")",
        R"(,"key_generation":"7")",
        ",\"condemned\":0",
        ",\"pending\":0",
        R"(,"oldest_round":"18446744073709551615")"})
    {
        String malformed = valid;
        eraseRequiredField(malformed, field);
        cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
            [&] { decodeFoldSeal(malformed, layout, 1); }, "missing");
    }

    /// `shard` occurs once on each row; remove each occurrence independently.
    String missing_blob_run_shard = valid;
    eraseRequiredField(missing_blob_run_shard, ",\"shard\":0");
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(missing_blob_run_shard, layout, 1); }, "missing");

    String missing_condemned_shard = valid;
    const size_t first_shard = missing_condemned_shard.find(",\"shard\":0");
    ASSERT_NE(first_shard, String::npos);
    const size_t second_shard = missing_condemned_shard.find(",\"shard\":0", first_shard + 1);
    ASSERT_NE(second_shard, String::npos);
    missing_condemned_shard.erase(second_shard, std::string_view(",\"shard\":0").size());
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeFoldSeal(missing_condemned_shard, layout, 1); }, "missing");
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
        .key_generation = 7});
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
    in.ref_lives[UInt128{3}].coverage = RefCoverage{.classification = CoverageClass::Absent};
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
    s.ref_lives[UInt128{1}].coverage = RefCoverage{.classification = CoverageClass::Folded};
    s.blob_target_runs.push_back(RunRef{.key = "gc/gen/9/blob_target/0/0", .checksum = UInt128(0x77),
                                        .shard = 0, .key_generation = 9});
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
            .classification = CoverageClass::Clamped,
            .last_folded_ref_id = RefTxnId{3, 4},
            .hold = RefHold{
                .reason = HoldReason::ManifestBodyMissing,
                .offending_position = RefTxnId{5, 6},
                .retry_count = 7,
                .next_retry_round = 8}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{9, 10}}});

    const String expected = currentFormatHeader("cas_fold_seal") +
        "{\"generation\":\"8\",\"parent_generation\":\"7\"}\n"
        "{\"kind\":\"ref_life\",\"life\":\"00000000000000000000000000001234\",\"class\":\"clamped\","
        "\"fold_epoch\":\"3\",\"fold_seq\":\"4\",\"hold_reason\":\"manifest_body_missing\",\"hold_epoch\":\"5\","
        "\"hold_seq\":\"6\",\"retries\":7,\"retry_round\":\"8\",\"remove_epoch\":\"9\",\"remove_seq\":\"10\"}\n"
        "{\"n\":1}\n";

    EXPECT_EQ(encodeFoldSeal(seal), expected);
    EXPECT_EQ(decodeFoldSeal(expected), seal);
}

/// Closed-set pin: `CoverageClass` and `HoldReason`
/// each walked through `magic_enum::enum_values`, which is what proves the renderer and the parser
/// consult the SAME table: a table entry missing altogether is already a build error at the
/// coverage assert, but two delegates drifting onto different tables is not.
TEST(CASFoldSealFormat, ClosedSetPinsCoverageClassAndHoldReasonWords)
{
    EXPECT_EQ(coverageClassToWord(CoverageClass::Absent), "absent");
    EXPECT_EQ(coverageClassToWord(CoverageClass::Unchanged), "unchanged");
    EXPECT_EQ(coverageClassToWord(CoverageClass::Folded), "folded");
    EXPECT_EQ(coverageClassToWord(CoverageClass::Clamped), "clamped");
    for (const auto c : magic_enum::enum_values<CoverageClass>())
        EXPECT_EQ(coverageClassFromWord(coverageClassToWord(c)), c);

    EXPECT_EQ(holdReasonToWord(HoldReason::GapBelowWitness), "gap_below_witness");
    EXPECT_EQ(holdReasonToWord(HoldReason::UnconsumedSealCrossing), "unconsumed_seal_crossing");
    EXPECT_EQ(holdReasonToWord(HoldReason::WitnessDisappeared), "witness_disappeared");
    EXPECT_EQ(holdReasonToWord(HoldReason::BodyUndecodable), "body_undecodable");
    EXPECT_EQ(holdReasonToWord(HoldReason::ManifestBodyMissing), "manifest_body_missing");
    EXPECT_EQ(holdReasonToWord(HoldReason::CheckpointUndecodable), "checkpoint_undecodable");
    for (const auto r : magic_enum::enum_values<HoldReason>())
        EXPECT_EQ(holdReasonFromWord(holdReasonToWord(r)), r);
}

/// Mutation caught: accepting the retired split coverage-collection kind would revive a second
/// namespace-keyed source of lifecycle work alongside the unified per-life row.
TEST(CASFoldSealFormat, UnifiedCodecRejectsLegacyCoverageRecord)
{
    const String old =
        "{\"type\":\"cas_fold_seal\",\"v\":1}\n"
        "{\"generation\":\"8\",\"parent_generation\":\"7\"}\n"
        "{\"kind\":\"cov\",\"key\":\"name/0\",\"class\":\"folded\",\"fold_epoch\":\"3\",\"fold_seq\":\"4\"}\n"
        "{\"n\":1}\n";
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(old); }, "legacy coverage");
}

/// Mutation caught: accepting the retired cleanup-item kind would restore the independent
/// marker-driven `Pending`/`Completed` handshake the unified row replaced.
TEST(CASFoldSealFormat, UnifiedCodecRejectsLegacyNamespaceCleanupRecord)
{
    const String old =
        "{\"type\":\"cas_fold_seal\",\"v\":1}\n"
        "{\"generation\":\"8\",\"parent_generation\":\"7\"}\n"
        "{\"kind\":\"nsc\",\"ns\":\"name\",\"remove_epoch\":\"3\",\"remove_seq\":\"4\",\"st\":\"completed\"}\n"
        "{\"n\":1}\n";
    cas_battery_detail::expectCode(
        DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(old); }, "legacy namespace cleanup");
}
