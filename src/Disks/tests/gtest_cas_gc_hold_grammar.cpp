#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasByteBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>
#include "cas_test_helpers.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

/// DURABLE HOLDS (spec 2026-07-27 "ref chain complete cut" §5).
///
/// A namespace whose ref-log walk meets an IMPOSSIBLE shape stops there, and that stop has to survive
/// the round. Before this task the stop was a single bit — `classification == 4` — and everything that
/// explained it (what went wrong, and exactly WHERE) lived in a log line and an in-memory anomaly, both
/// gone by the next round. That is not enough for three separate reasons:
///
///   * the next round could not RETRY the exact position, so a hold only survived while the round's
///     hint happened to keep mentioning the namespace;
///   * the hold could be cleared by an ABSENT — precisely the observation a lying store produces, and
///     precisely the shape that made the hold necessary in the first place;
///   * REBUILD rewrote coverage from owner state and silently dropped every hold, handing back a
///     baseline that looked proven when it was not.
///
/// So the hold is now DURABLE and STRICTLY GRAMMARED: `{reason, offending_position, retry_count,
/// next_retry_round}` present if and only if `classification == 4`, rejected in both directions
/// otherwise. It rides the seal across rounds — including rounds whose hint omits the namespace
/// entirely — and across REBUILD, and it clears by exactly ONE event: the fold resolving the offending
/// position and that result being adopted in `gc/state`.
///
/// The carried hold is also a WITNESS, and a better one than the listing: it is durable proof that the
/// walk once reached that position, so an absent below it is a gap rather than a frontier no matter
/// what the hint says this round. That is what makes "retry the exact offending position" work for a
/// hold that sits above an epoch boundary.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

namespace ProfileEvents
{
extern const Event CASGCRebuildVirginByEnumeration;
}

namespace
{

const UInt128 kGc = hexToU128("00000000000000000000000000000001");

/// ===================== FIXTURES =====================

/// A backend that hides keys from every LIST while serving them by exact key (the observed lying-store
/// shape) AND counts reads. The hold tests need both: the hint has to go quiet while the exact GET the
/// hold forces stays observable.
class HintHoleCountingBackend : public CountingBackend
{
public:
    void hide(const String & key)
    {
        std::lock_guard lock(m);
        hidden.insert(key);
    }

    size_t holesServed() const
    {
        std::lock_guard lock(m);
        return served;
    }

    ListPage list(const String & prefix, const String & cursor, size_t limit) override
    {
        ListPage page = CountingBackend::list(prefix, cursor, limit);
        std::lock_guard lock(m);
        if (hidden.empty())
            return page;
        const size_t before = page.keys.size();
        std::erase_if(page.keys, [&](const ListedKey & k) { return hidden.contains(k.key); });
        if (page.keys.size() != before)
            ++served;
        return page;
    }

private:
    mutable std::mutex m;
    std::set<String> hidden;
    size_t served = 0;
};

/// Write the namespace's `_ckpt` naming `checkpoint` as its snapshot base, through the real codec — the
/// fold's second witness source is a decode of exactly these bytes, so a hand-rolled body would prove
/// nothing about the object the writers actually publish.
void writeCkptAt(
    Backend & backend, const Layout & layout, const RootNamespace & ns, const RefTxnId & checkpoint)
{
    writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = checkpoint,
        .checkpoint_snapshot_id = checkpoint,
        .last_epoch_seal = std::nullopt,
    });
}

/// Establish only the immutable recovery frontier for a raw-log fixture. Unlike `writeCkptAt`, this
/// does not claim a snapshot exists: rebuild tests need to replay the log through this exact position.
void writeCommittedCkptAt(
    Backend & backend, const Layout & layout, const RootNamespace & ns, const RefTxnId & committed_through)
{
    writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = committed_through,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
}

/// The newest fold seal, scanning downward from the adopted generation (a completed round's gc/state
/// points at the recheck generation).
std::optional<CasFoldSeal> newestSeal(Backend & backend, const Layout & layout)
{
    const uint64_t gen = currentGenerationOf(backend, layout);
    const uint64_t attempt = currentAttemptOf(backend, layout);
    for (uint64_t g = gen; ; --g)
    {
        if (const auto got = backend.get(layout.foldSealKey(g, attempt)))
            return decodeFoldSeal(got->bytes);
        if (g == 0)
            return std::nullopt;
    }
}

std::optional<RefCoverage> coverageOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const auto seal = newestSeal(backend, layout);
    if (!seal)
        return std::nullopt;
    const auto it = seal->ref_lives.find(catalogLifeIdForTest(backend, layout, ns));
    if (it == seal->ref_lives.end())
        return std::nullopt;
    return it->second.coverage;
}

/// The cursor `ns` was sealed at, or `{0, 0}` when the round sealed NO row for it at all. It never
/// dereferences a disengaged optional: a test that aborts the process takes every test after it in the
/// binary down with it, and "there is no coverage row" is exactly the shape a regression in the hold
/// carry produces — so it has to read as a failed expectation, not as a crash that hides the rest.
RefTxnId sealedCursorOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const auto cov = coverageOf(backend, layout, ns);
    EXPECT_TRUE(cov.has_value()) << "no coverage row for " << ns.string();
    return cov ? cov->last_folded_ref_id : RefTxnId{};
}

/// The coverage row a round MUST have sealed for `ns`, held. Fails the test rather than returning an
/// empty optional, so every caller below reads a real hold.
RefHold holdOf(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    const auto cov = coverageOf(backend, layout, ns);
    EXPECT_TRUE(cov.has_value()) << "no coverage row for " << ns.string();
    if (!cov)
        return RefHold{};
    EXPECT_EQ(cov->classification, 4) << "a held namespace is classification 4";
    EXPECT_TRUE(cov->hold.has_value()) << "classification 4 without a hold is the forbidden shape";
    return cov->hold ? *cov->hold : RefHold{};
}

UInt128 fixtureLifeId(std::string_view key)
{
    return key.ends_with("/1") ? UInt128{2} : UInt128{1};
}

RefCoverage & fixtureCoverage(CasFoldSeal & seal, std::string_view key)
{
    return seal.ref_lives[fixtureLifeId(key)].coverage;
}

const RefCoverage & fixtureCoverage(const CasFoldSeal & seal, std::string_view key)
{
    return seal.ref_lives.at(fixtureLifeId(key)).coverage;
}

/// A seal carrying exactly one held coverage row with every numeric at its maximum and a coverage key
/// that needs escaping — the widest row the per-row line budget has to survive. The caller supplies the
/// key, which is what the line-cap tests below grow byte by byte.
CasFoldSeal maximalHoldSeal(const String & map_key)
{
    CasFoldSeal seal;
    seal.generation = std::numeric_limits<uint64_t>::max();
    seal.parent_generation = std::numeric_limits<uint64_t>::max();
    RefCoverage cov;
    cov.classification = 4;
    cov.last_folded_ref_id = RefTxnId{std::numeric_limits<uint64_t>::max(),
                                      std::numeric_limits<uint64_t>::max()};
    cov.hold = RefHold{.reason = HoldReason::UnconsumedSealCrossing,   /// the longest reason word
                       .offending_position = RefTxnId{std::numeric_limits<uint64_t>::max(),
                                                      std::numeric_limits<uint64_t>::max()},
                       .retry_count = std::numeric_limits<uint32_t>::max(),
                       .next_retry_round = std::numeric_limits<uint64_t>::max()};
    fixtureCoverage(seal, map_key) = cov;
    return seal;
}

/// The `cov` line of an encoded seal (line 3: header, meta, then the single record).
String covLineOf(const String & encoded)
{
    size_t begin = encoded.find('\n') + 1;   /// past the header
    begin = encoded.find('\n', begin) + 1;   /// past the meta line
    return encoded.substr(begin, encoded.find('\n', begin) - begin);
}

/// A one-row seal whose coverage is ordinary and CLEAN: folded through its cursor, nothing held.
CasFoldSeal cleanSeal(const String & map_key)
{
    CasFoldSeal seal;
    seal.generation = 3;
    seal.parent_generation = 2;
    RefCoverage cov;
    cov.classification = 2;
    cov.last_folded_ref_id = RefTxnId{4, 5};
    fixtureCoverage(seal, map_key) = cov;
    return seal;
}

/// A one-row seal whose coverage is HELD at an exact position — the row every erasure shape below is
/// trying to make disappear.
CasFoldSeal heldSeal(const String & map_key)
{
    CasFoldSeal seal = cleanSeal(map_key);
    RefCoverage & cov = fixtureCoverage(seal, map_key);
    cov.classification = 4;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{4, 6},
                       .retry_count = 7, .next_retry_round = 99};
    return seal;
}

/// The header and meta lines (1 and 2) of an encoded seal, terminators included.
String headerAndMetaOf(const String & encoded)
{
    const size_t past_meta = encoded.find('\n', encoded.find('\n') + 1) + 1;
    EXPECT_NE(past_meta, 0u);
    return encoded.substr(0, past_meta);
}

/// Assemble a raw seal object from `records` (one record per element, no terminators), on `prototype`'s
/// header and meta lines, closed by the trailer count those records imply. This is the ONLY way to put a
/// repeated record key on the wire: `CasFoldSeal` stores keyed maps, so a duplicate is not a value any
/// producer can hold — it is a shape a forged, truncated, or mis-merged object has.
String sealTextWith(const String & prototype, const std::vector<String> & records)
{
    String text = headerAndMetaOf(prototype);
    for (const String & record : records)
        text += record + "\n";
    return text + "{\"n\":" + std::to_string(records.size()) + "}\n";
}

/// Replace the coverage row's `cls` value with `raw`, VERBATIM. The point is to write integers no
/// `RefCoverage` can hold: the field is a byte in the struct, so a wide value exists only on the wire,
/// which is exactly where a reader has to catch it. `cls` is never the last field of a `cov` record, so
/// the value always ends at a comma.
String withRawClassification(const String & encoded, std::string_view raw)
{
    const size_t at = encoded.find("\"cls\":");
    EXPECT_NE(at, String::npos);
    const size_t begin = at + strlen("\"cls\":");
    const size_t end = encoded.find(',', begin);
    EXPECT_NE(end, String::npos);
    return encoded.substr(0, begin) + String{raw} + encoded.substr(end);
}

/// Replace the FIRST occurrence of `field` with `replacement` (both are whole `"key":value` fragments),
/// so a test states the exact wire shape it is feeding the decoder.
String withField(const String & encoded, const String & field, const String & replacement)
{
    const size_t at = encoded.find(field);
    EXPECT_NE(at, String::npos) << "the encoder does not emit " << field;
    return encoded.substr(0, at) + replacement + encoded.substr(at + field.size());
}

/// Every coverage row the ENCODER must refuse, each paired with why producing it would be a bug in our
/// own fold rather than corruption arriving from a store. Shared by the two builds' assertions below so
/// the release expectation and the sanitizer death expectation can never drift apart.
std::vector<std::pair<const char *, CasFoldSeal>> illFormedSealsTheEncoderMustRefuse()
{
    std::vector<std::pair<const char *, CasFoldSeal>> out;

    /// The pairing, both ways round.
    CasFoldSeal hold_on_folded = heldSeal("ns/0");
    fixtureCoverage(hold_on_folded, "ns/0").classification = 2;
    out.emplace_back("a hold on a folded (2) row claims a stop that did not happen", hold_on_folded);

    CasFoldSeal clamped_without_hold = heldSeal("ns/0");
    fixtureCoverage(clamped_without_hold, "ns/0").hold.reset();
    out.emplace_back("a clamped (4) row with no hold is indistinguishable from a clean cursor once "
                     "durable", clamped_without_hold);

    /// The closed set. 3 is the dangerous one: it passes the sweep's `== 4` and `== 0` refusals and
    /// reaches the deletion premise, which is a refusal written in terms of the set.
    CasFoldSeal classification_three = cleanSeal("ns/0");
    fixtureCoverage(classification_three, "ns/0").classification = 3;
    out.emplace_back("classification 3 is not one of {0,1,2,4} and passes every refusal stated in terms "
                     "of them", classification_three);

    CasFoldSeal classification_max = cleanSeal("ns/0");
    fixtureCoverage(classification_max, "ns/0").classification = 255;
    out.emplace_back("classification 255 is not one of {0,1,2,4}", classification_max);

    /// The self-erasing hold, and its half-zero sibling.
    CasFoldSeal hold_at_zero = heldSeal("ns/0");
    fixtureCoverage(hold_at_zero, "ns/0").hold->offending_position = RefTxnId{};
    out.emplace_back("a hold at {0,0} is cleared by the first record the next round folds", hold_at_zero);

    CasFoldSeal hold_zero_sequence = heldSeal("ns/0");
    fixtureCoverage(hold_zero_sequence, "ns/0").hold->offending_position = RefTxnId{7, 0};
    out.emplace_back("a hold position with a zero component is not a renderable id", hold_zero_sequence);

    return out;
}

}

/// ===================== THE SHARED BYTE ARITHMETIC =====================
///
/// Two caps, two predicates, one place they are computed. Stage B's catalog reuses THESE functions for
/// its additive "does one more entry still fit" question, so their boundary behaviour is pinned here
/// rather than re-derived per format: a cap is the largest PERMITTED value (equality fits), and every
/// sum saturates, because a wrapped sum answers "fits" for an object that does not — turning an
/// overflow into a durable object nothing can read.
TEST(CASGCHoldGrammarBudget, BothPredicatesAcceptEqualityAndRefuseOneMore)
{
    static_assert(fitsLineCap(64, 64));
    static_assert(!fitsLineCap(65, 64));
    static_assert(fitsObjectCap(40, 24, 64));
    static_assert(!fitsObjectCap(40, 25, 64));

    EXPECT_TRUE(fitsLineCap(64, 64));
    EXPECT_FALSE(fitsLineCap(65, 64));
    EXPECT_TRUE(fitsObjectCap(64, 0, 64));
    EXPECT_FALSE(fitsObjectCap(64, 1, 64));

    /// A cap of 0 means the format declares none (a streamed object never materialized whole).
    EXPECT_TRUE(fitsLineCap(std::numeric_limits<uint64_t>::max(), 0));
    EXPECT_TRUE(fitsObjectCap(std::numeric_limits<uint64_t>::max(), 1, 0));
}

TEST(CASGCHoldGrammarBudget, SumsSaturateInsteadOfWrapping)
{
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
    static_assert(addByteBudget(kMax, 1) == kMax);
    static_assert(addByteBudget(kMax, kMax) == kMax);
    static_assert(addByteBudget(3, 4) == 7);

    /// The predicate that matters: a reservation that would wrap must REFUSE, not report a tiny sum.
    EXPECT_FALSE(fitsObjectCap(kMax, 2, 256 * 1024 * 1024));
}

/// ===================== THE STRICT CLASSIFICATION-4 GRAMMAR =====================

TEST(CASGCHoldGrammar, EveryHoldReasonRoundTrips)
{
    for (const HoldReason reason : {HoldReason::GapBelowWitness, HoldReason::UnconsumedSealCrossing,
                                    HoldReason::WitnessDisappeared, HoldReason::BodyUndecodable,
                                    HoldReason::ManifestBodyMissing, HoldReason::CheckpointUndecodable})
    {
        CasFoldSeal seal;
        seal.generation = 3;
        seal.parent_generation = 2;
        RefCoverage cov;
        cov.classification = 4;
        cov.last_folded_ref_id = RefTxnId{4, 5};
        cov.hold = RefHold{.reason = reason, .offending_position = RefTxnId{4, 6},
                           .retry_count = 7, .next_retry_round = 99};
        fixtureCoverage(seal, "ns/0") = cov;

        const CasFoldSeal back = decodeFoldSeal(encodeFoldSeal(seal));
        EXPECT_EQ(back, seal) << "hold reason " << static_cast<int>(reason);
        ASSERT_TRUE(fixtureCoverage(back, "ns/0").hold.has_value());
        EXPECT_EQ(fixtureCoverage(back, "ns/0").hold->reason, reason);
    }
}

/// THE ENCODER'S HALF OF THE GRAMMAR, in one place. Every shape here is OUR OWN fold handing the codec
/// a row it must never make durable, so the refusal is `LOGICAL_ERROR` — the code `encodeGcState` raises
/// for the same category of impossible input — and not the `CORRUPTED_DATA` reserved for bytes that
/// arrived from a store. Under a debug or sanitizer build that code ABORTS at construction
/// (`handle_error_code`), so the same table is asserted as a death expectation there; the contract
/// ("these bytes are never produced") is what both forms pin.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASGCHoldGrammar, TheEncoderRefusesEveryIllFormedCoverageRow)
{
    for (const auto & entry : illFormedSealsTheEncoderMustRefuse())
    {
        SCOPED_TRACE(entry.first);
        expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeFoldSeal(entry.second); });
    }
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASGCHoldGrammarDeathTest, TheEncoderRefusesEveryIllFormedCoverageRow)
{
    for (const auto & entry : illFormedSealsTheEncoderMustRefuse())
    {
        SCOPED_TRACE(entry.first);
        EXPECT_DEATH({ (void)encodeFoldSeal(entry.second); }, "");
    }
}
#endif

TEST(CASGCHoldGrammar, AHoldOnAnyOtherClassificationIsRefusedByTheDecoder)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;

    /// Bytes some other producer wrote. Built by demoting a legitimate held row's classification, so the
    /// hold fields are exactly the ones the encoder emits.
    cov.classification = 4;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 1};
    fixtureCoverage(seal, "ns/0") = cov;
    String text = encodeFoldSeal(seal);
    const size_t at = text.find("\"cls\":4");
    ASSERT_NE(at, String::npos);
    text[at + 6] = '2';
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(text); });
}

TEST(CASGCHoldGrammar, ClassificationFourWithoutAHoldIsRefusedByTheDecoder)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = 4;
    cov.last_folded_ref_id = RefTxnId{1, 1};

    /// Every single hold field is REQUIRED: dropping any one of them is corruption, not a default.
    cov.hold = RefHold{.reason = HoldReason::BodyUndecodable, .offending_position = RefTxnId{1, 2},
                       .retry_count = 3, .next_retry_round = 4};
    fixtureCoverage(seal, "ns/0") = cov;
    const String whole = encodeFoldSeal(seal);
    for (const String & field : {String(R"("hr":"body_undecodable")"), String(R"("hpe":"1")"),
                                 String(R"("hps":"2")"), String(R"("hrc":3)"), String(R"("hnr":"4")")})
    {
        SCOPED_TRACE("without " + field);
        const size_t at = whole.find(field);
        ASSERT_NE(at, String::npos) << "the encoder does not emit " << field;
        String without = whole;
        without.erase(at - 1, field.size() + 1);   /// the field and the ',' before it
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(without); });
    }
}

TEST(CASGCHoldGrammar, DuplicateHoldKeyIsCorruptedData)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = 4;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 5};
    fixtureCoverage(seal, "ns/0") = cov;

    const String whole = encodeFoldSeal(seal);
    const String field = R"("hr":"gap_below_witness")";
    const size_t at = whole.find(field);
    ASSERT_NE(at, String::npos);
    /// The same key twice, with a DIFFERENT value: last-wins would silently rewrite the reason.
    String doubled = whole;
    doubled.insert(at, R"("hr":"witness_disappeared",)");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(doubled); });
}

TEST(CASGCHoldGrammar, UnknownHoldReasonWordIsCorruptedData)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = 4;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 5};
    fixtureCoverage(seal, "ns/0") = cov;

    String text = encodeFoldSeal(seal);
    const size_t at = text.find("gap_below_witness");
    ASSERT_NE(at, String::npos);
    text.replace(at, strlen("gap_below_witness"), "gap_below_witnesX");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(text); });
}

/// ===================== THE THREE WAYS A SEAL CAN ERASE A HOLD =====================
///
/// The three shapes below are one finding, and it is about what a fold seal is FOR. The hold is the only
/// durable record that a namespace stopped and where; everything downstream reads the seal and nothing
/// re-derives the stop. So a seal that decodes into "no hold here" is not a lossy read, it is a licence
/// to delete: the sweep's §6 refusals are stated as `classification == 4` / `== 0` / `hold.has_value()`,
/// and a row that slips past all three reaches an irreversible delete of a manifest the fold never
/// accounted for. Each shape gets past a DIFFERENT one of the decoder's checks, which is why they are
/// pinned separately rather than as one "malformed seal" case.

/// (1) The classification the reader never sees. `cls` is narrowed to a byte, so an integer on the wire
/// is truncated first and validated (if at all) afterwards: 258 becomes 2, "everything through the
/// cursor was folded". The value has to be judged WIDE, before the narrowing, or the wire can buy
/// coverage that no fold ever performed.
TEST(CASGCHoldGrammar, AClassificationOutsideTheGrammarIsCorruptedData)
{
    const String clean = encodeFoldSeal(cleanSeal("ns/0"));
    ASSERT_EQ(fixtureCoverage(decodeFoldSeal(clean), "ns/0").classification, 2)
        << "the unmodified row is the one every case below deviates from";

    /// In-range bytes that are simply not classifications. 3 is the one the sweep's refusals miss.
    for (const std::string_view raw : {"3", "5", "6", "255"})
    {
        SCOPED_TRACE(String{"cls="} + String{raw});
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                         [&] { decodeFoldSeal(withRawClassification(clean, raw)); });
    }

    /// Wide integers whose LOW BYTE lands inside the grammar: 258 -> 2 (fully folded), 256 -> 0
    /// (absent), 260 -> 4 (clamped). Each would decode as a row the fold never wrote.
    for (const std::string_view raw : {"256", "258", "260", "18446744073709551615"})
    {
        SCOPED_TRACE(String{"cls="} + String{raw});
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                         [&] { decodeFoldSeal(withRawClassification(clean, raw)); });
    }
}

/// And the field itself is required: an absent `cls` reads as 0, which is not "nothing was said about
/// this namespace" but the positive claim "no round folded it".
TEST(CASGCHoldGrammar, ACoverageRowWithoutAClassificationIsCorruptedData)
{
    const String clean = encodeFoldSeal(cleanSeal("ns/0"));
    const size_t at = clean.find("\"cls\":2,");
    ASSERT_NE(at, String::npos);
    String without = clean;
    without.erase(at, strlen("\"cls\":2,"));
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(without); });
}

/// (2) The hold that clears itself. `{0,0}` passes the completeness check — every field is present — and
/// then the carry rule drops it on the next round, because a hold rides forward only while the walk
/// stops BELOW its position and nothing is below zero. The namespace advances with no record that it was
/// ever held. A zero in EITHER component is the same defect, and is additionally unnameable: the sweep
/// renders the position when it reports what it retained, and `renderRefTxnId` refuses a zero component.
TEST(CASGCHoldGrammar, AHoldWhoseOffendingPositionHasAZeroComponentIsCorruptedData)
{
    const String held = encodeFoldSeal(heldSeal("ns/0"));
    ASSERT_TRUE(fixtureCoverage(decodeFoldSeal(held), "ns/0").hold.has_value());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        decodeFoldSeal(withField(withField(held, R"("hpe":"4")", R"("hpe":"0")"),
                                 R"("hps":"6")", R"("hps":"0")"));
    });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(held, R"("hpe":"4")", R"("hpe":"0")")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(held, R"("hps":"6")", R"("hps":"0")")); });
}

/// (3) The duplicate row. Two `cov` records for the same (namespace, shard) — held first, clean second —
/// used to be accepted with last-wins, so a single appended line erased a hold without touching the one
/// that recorded it. There is exactly one row per key, and a second one is corruption.
TEST(CASGCHoldGrammar, ASecondCoverageRowForTheSameKeyIsCorruptedData)
{
    const String held_line = covLineOf(encodeFoldSeal(heldSeal("ns/0")));
    const String clean_line = covLineOf(encodeFoldSeal(cleanSeal("ns/0")));
    const String other_clean_line = covLineOf(encodeFoldSeal(cleanSeal("ns/1")));
    const String prototype = encodeFoldSeal(heldSeal("ns/0"));

    /// The CONTROL first: the same two-record assembly with DIFFERENT keys decodes, so the refusal below
    /// is about the repeated key and not about the way these bytes are forged.
    const CasFoldSeal two_keys = decodeFoldSeal(sealTextWith(prototype, {held_line, other_clean_line}));
    ASSERT_EQ(two_keys.ref_lives.size(), 2u);
    ASSERT_TRUE(fixtureCoverage(two_keys, "ns/0").hold.has_value());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {held_line, clean_line})); });
    /// Order does not redeem it: a clean row followed by a held one is the same broken object.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {clean_line, held_line})); });
    /// Nor does repeating the identical row.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {held_line, held_line})); });

    /// The ENCODER needs no matching check, and this is why: the seal stores keyed maps, so a second row
    /// for a key is not a value any producer can construct — assigning it replaces the first.
    CasFoldSeal seal = heldSeal("ns/0");
    fixtureCoverage(seal, "ns/0") = fixtureCoverage(cleanSeal("ns/0"), "ns/0");
    EXPECT_EQ(seal.ref_lives.size(), 1u);
}

/// The same one-record-per-key rule applies to `cnd`: a repeated row rewrites a shard's condemned
/// totals, which graduation paces on.
TEST(CASGCHoldGrammar, ASecondCondemnedSummaryRecordIsCorruptedData)
{
    CasFoldSeal seal = cleanSeal("ns/0");
    seal.condemned_summary[0] = CondemnedSummary{.condemned_total = 5, .pending_total = 1,
                                                 .oldest_nonpending_condemn_round = 3};
    const String encoded = encodeFoldSeal(seal);

    /// Lines 3..4 are `rfl`, `cnd` in the encoder's fixed order.
    std::vector<String> lines;
    for (size_t begin = headerAndMetaOf(encoded).size(); begin < encoded.size();)
    {
        const size_t end = encoded.find('\n', begin);
        ASSERT_NE(end, String::npos);
        lines.push_back(encoded.substr(begin, end - begin));
        begin = end + 1;
    }
    ASSERT_EQ(lines.size(), 3u) << "rfl, cnd and the trailer";
    const String ref_life_line = lines[0];
    const String cnd_line = lines[1];

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(encoded, {ref_life_line, cnd_line, cnd_line})); });
    /// The unduplicated assembly is the control.
    const std::vector<String> one_of_each{ref_life_line, cnd_line};
    EXPECT_NO_THROW(decodeFoldSeal(sealTextWith(encoded, one_of_each)));
}

/// Unified cleanup evidence still requires a canonical nonzero removal transaction id. Decoding
/// foreign bytes must fail this read, never the process.
TEST(CASGCHoldGrammar, CleanupEvidenceWithAZeroRemovalIdIsCorruptedData)
{
    CasFoldSeal seal = cleanSeal("ns/0");
    seal.ref_lives.at(fixtureLifeId("ns/0")).cleanup_evidence =
        RefCleanupEvidence{.remove_txn_id = RefTxnId{2, 3}};
    const String encoded = encodeFoldSeal(seal);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("rte":"2")", R"("rte":"0")")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("rts":"3")", R"("rts":"0")")); });
    /// Omitted entirely is the same thing: the fields default to zero.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("rte":"2",)", "")); });
}

/// The OBJECT cap bounds the whole seal. Nothing on the fold-seal READ path enforces it (the seal
/// is read raw, never through `openObject`), so an oversized PUT would leave a durable seal that no
/// later round can decode — unrecoverable. The gate therefore sits before the bytes are handed out, and
/// equality is still accepted: the cap is the largest permitted size, not the first forbidden one.
TEST(CASGCHoldGrammar, ObjectCapAcceptsEqualityAndRefusesOneMoreByte)
{
    const uint64_t object_cap = foldSealCaps().object_cap;
    ASSERT_EQ(object_cap, 256u * 1024 * 1024);

    EXPECT_NO_THROW(checkFoldSealObjectBytes(object_cap - 1));
    EXPECT_NO_THROW(checkFoldSealObjectBytes(object_cap));
    expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED, [&] { checkFoldSealObjectBytes(object_cap + 1); });

    /// An ordinary seal is nowhere near it, so the gate costs a comparison and changes nothing.
    EXPECT_NO_THROW(encodeFoldSeal(maximalHoldSeal("ns/0")));
}

/// ===================== HOLDS ARE CREATED WITH AN EXACT POSITION =====================

TEST(CASGCHoldGrammar, GapBelowWitnessNamesTheExactAbsentPosition)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    /// {1,3} never existed; {1,4} is durable AND listed, so the gap is impossible under contiguity.
    publishAt(*backend, layout, ns, RefTxnId{1, 4}, "ref_4", 4, DB::UInt128(4));
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 4});

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const RefHold hold = holdOf(*backend, layout, ns);
    EXPECT_EQ(hold.reason, HoldReason::GapBelowWitness);
    EXPECT_EQ(hold.offending_position, (RefTxnId{1, 3}));
    EXPECT_EQ(hold.retry_count, 0u) << "the round that creates a hold has retried nothing yet";
    EXPECT_GT(hold.next_retry_round, 0u);
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));
}

TEST(CASGCHoldGrammar, UnconsumedSealCrossingNamesTheAbsentPosition)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    /// Epoch 1 ends at {1,1} with NO seal, and epoch 2 chains to a seal at {1,3} that this cursor
    /// never consumed (and that does not exist). The nearest witness above the absent {1,2} therefore
    /// sits in another epoch, and the crossing has nothing to prove itself from.
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 3});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{1, 3},
    });

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const RefHold hold = holdOf(*backend, layout, ns);
    EXPECT_EQ(hold.reason, HoldReason::UnconsumedSealCrossing);
    EXPECT_EQ(hold.offending_position, (RefTxnId{1, 2})) << "the hold names the position that read absent";
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(2)), 0)
        << "nothing beyond the unproven boundary may fold";
}

TEST(CASGCHoldGrammar, UndecodableBodyNamesTheRecordItCouldNotRead)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    backend->putIfAbsent(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 2}), "this is not a cas_ref_log object");
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 2});

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const RefHold hold = holdOf(*backend, layout, ns);
    EXPECT_EQ(hold.reason, HoldReason::BodyUndecodable);
    EXPECT_EQ(hold.offending_position, (RefTxnId{1, 2}));
}

/// The fold barrier is a hold too, and it is the ONE hold whose ordinary cause is benign: a writer that
/// has appended its precommit record but not yet finished uploading the manifest body. It gets the same
/// durable treatment as the corruption shapes because it stops the namespace the same way — and because
/// a barrier that is durably named is one an operator can distinguish from a wedge.
TEST(CASGCHoldGrammar, MissingManifestBodyBarrierIsADurableHold)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    deleteManifestBody(*backend, layout,
                       ManifestId{ns, ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1}});
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 2});

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const RefHold hold = holdOf(*backend, layout, ns);
    EXPECT_EQ(hold.reason, HoldReason::ManifestBodyMissing);
    EXPECT_EQ(hold.offending_position, (RefTxnId{1, 2})) << "the hold names the LOG whose edges could not fold";
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 1}));
}

/// An above-cursor record that answered one GET and then stopped answering is CORRUPTION, not a
/// frontier: nothing may legitimately remove an object above the fold cursor. It is the one hold shape
/// that no amount of waiting can clear, and naming it durably is what stops a later round from reading
/// the same namespace as quiet and granting it a frontier proof.
TEST(CASGCHoldGrammar, AWitnessThatStopsAnsweringIsWitnessDisappeared)
{
    /// Answers on odd-numbered reads and 404s on even ones: `crossFromSeal` proves the position, and
    /// the walk's own GET of it then fails.
    class AlternatingGetBackend : public InMemoryBackend
    {
    public:
        using DB::Cas::Backend::get;
        String flaky;
        size_t reads = 0;

        std::optional<GetResult> get(const String & key, Range range) override
        {
            if (key == flaky && ++reads % 2 == 0)
                return std::nullopt;
            return InMemoryBackend::get(key, range);
        }
    };

    auto backend = std::make_shared<AlternatingGetBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeSealAt(*backend, layout, ns, RefTxnId{1, 2});
    publishAt(*backend, layout, ns, RefTxnId{2, 1}, "ref_2", 2, DB::UInt128(2),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{1, 2});
    /// A third epoch keeps the unstable position from reading as a frontier.
    publishAt(*backend, layout, ns, RefTxnId{3, 1}, "ref_3", 3, DB::UInt128(3),
              /*birth=*/false, /*prev_epoch_seal=*/RefTxnId{2, 1});
    writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{3, 1},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = RefTxnId{2, 1},
    });
    backend->flaky = layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{2, 1});

    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const RefHold hold = holdOf(*backend, layout, ns);
    EXPECT_EQ(hold.reason, HoldReason::WitnessDisappeared);
    /// The walk crossed into epoch 2 on the record's first answer and then could not read it: the hold
    /// names {2,1}, the position that stopped being readable, and the cursor stays on the seal below it.
    EXPECT_EQ(hold.offending_position, (RefTxnId{2, 1}));
    EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 2}));
}

/// ===================== THE SECOND WITNESS: `_ckpt.checkpoint` =====================
///
/// A listing is a SNAPSHOT: a record that became durable after the enumeration is invisible to that
/// round's probes, so an absent expected-next reads as a frontier when it is really a gap. The
/// namespace's own durable checkpoint decides the same question without asking the listing anything —
/// and this pair of pools is the proof, because they differ in nothing else.
TEST(CASGCHoldGrammar, CheckpointWitnessHoldsAGapTheHintIsSilentAbout)
{
    const RootNamespace ns{"00/aa@cas@"};
    /// Stage B (Task 4-C): no pin needed here -- `publishAt` below (draining into `writeRefLogTxnRaw`)
    /// admits `ns` into the catalog itself, once per pool, inside each nested block's own `seed` call.
    const auto seed = [&](HintHoleCountingBackend & backend, const Layout & layout)
    {
        publishAt(backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
        publishAt(backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
        /// {1,3} is missing and {1,4}, though durable, is invisible to every LIST.
        publishAt(backend, layout, ns, RefTxnId{1, 4}, "ref_4", 4, DB::UInt128(4));
        backend.hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 4}));
    };

    /// Hint-only: nothing above {1,2} is visible, so the walk honestly reads a frontier and does not hold.
    {
        auto backend = std::make_shared<HintHoleCountingBackend>();
        auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
        seed(*backend, store->layout());
        Gc gc(store, kGc);
        ASSERT_TRUE(gc.runRegularRound().acquired_lease);
        ASSERT_GT(backend->holesServed(), 0u);
        const auto cov = coverageOf(*backend, store->layout(), ns);
        ASSERT_TRUE(cov.has_value());
        EXPECT_FALSE(cov->hold.has_value()) << "without a witness an absent IS the frontier";
    }

    /// Same pool, same hint, plus the checkpoint: the gap becomes decidable and holds at the same
    /// position, with the same reason, as if the hint had shown the witness itself.
    ///
    /// The `_ckpt` object is hidden from every LIST as well, so the two pools' listings are byte-for-byte
    /// the same and the only difference between them is an object reachable by EXACT KEY alone. That is
    /// what makes this a proof of hint-INDEPENDENCE rather than of a richer hint.
    {
        auto backend = std::make_shared<HintHoleCountingBackend>();
        auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
        const Layout & layout = store->layout();
        seed(*backend, layout);
        writeCkptAt(*backend, layout, ns, RefTxnId{1, 4});
        backend->hide(layout.refCkptKey(fixture::fixtureLife(ns)));

        Gc gc(store, kGc);
        ASSERT_TRUE(gc.runRegularRound().acquired_lease);

        const RefHold hold = holdOf(*backend, layout, ns);
        EXPECT_EQ(hold.reason, HoldReason::GapBelowWitness);
        EXPECT_EQ(hold.offending_position, (RefTxnId{1, 3}));
    }
}

/// The namespace whose second witness matters MOST: one the hint has stopped mentioning entirely, kept
/// in the round's universe by nothing but its CARRIED HOLD. Its checkpoint is why
/// `readCheckpointWitnesses` takes the parent cursors as well as the hint — the hold alone witnesses only
/// the position it stopped at, so a gap ABOVE that position, once the hold resolves, has no witness left.
TEST(CASGCHoldGrammar, CheckpointWitnessReachesAHeldNamespaceTheHintNoLongerNames)
{
    const RootNamespace ns{"00/aa@cas@"};
    /// Stage B (Task 4-C): no pin needed -- `publishAt` inside `seedPool` (draining into
    /// `writeRefLogTxnRaw`) admits `ns` into each nested block's own pool.

    /// Round 1 in both pools: held at {1,3} by a gap below the listed witness {1,4}. Then the hint goes
    /// silent about every one of the namespace's objects, {1,3} becomes readable (so the hold resolves and
    /// the walk runs on), and a durable-but-unlisted {1,6} leaves a fresh gap at {1,5}.
    const auto seedPool = [&](HintHoleCountingBackend & backend, const Layout & layout, Gc & gc)
    {
        publishAt(backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
        publishAt(backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
        publishAt(backend, layout, ns, RefTxnId{1, 4}, "ref_4", 4, DB::UInt128(4));
        writeCommittedCkptAt(backend, layout, ns, RefTxnId{1, 4});
        EXPECT_TRUE(gc.runRegularRound().acquired_lease);
        EXPECT_EQ(holdOf(backend, layout, ns).offending_position, (RefTxnId{1, 3}));

        publishAt(backend, layout, ns, RefTxnId{1, 3}, "ref_3", 3, DB::UInt128(3));
        publishAt(backend, layout, ns, RefTxnId{1, 6}, "ref_6", 6, DB::UInt128(6));
        for (const RefTxnId & id : {RefTxnId{1, 1}, RefTxnId{1, 2}, RefTxnId{1, 3}, RefTxnId{1, 4},
                                    RefTxnId{1, 6}})
            backend.hide(layout.refLogKey(fixture::fixtureLife(ns), id));
    };

    /// Hold-witness only: it witnesses {1,3}, which the walk has now passed, so the absent {1,5} above it
    /// is an honest frontier and the namespace comes out clean.
    {
        auto backend = std::make_shared<HintHoleCountingBackend>();
        auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
        Gc gc(store, kGc);
        seedPool(*backend, store->layout(), gc);

        ASSERT_TRUE(gc.runRegularRound().acquired_lease);
        const auto cov = coverageOf(*backend, store->layout(), ns);
        ASSERT_TRUE(cov.has_value());
        EXPECT_FALSE(cov->hold.has_value()) << "a resolved hold witnesses nothing above itself";
        EXPECT_EQ(cov->last_folded_ref_id, (RefTxnId{1, 4}));
    }

    /// Same pool, plus the checkpoint — read by exact key for a namespace THIS round's hint never names.
    {
        auto backend = std::make_shared<HintHoleCountingBackend>();
        auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
        const Layout & layout = store->layout();
        Gc gc(store, kGc);
        seedPool(*backend, layout, gc);
        advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{1, 6});
        backend->hide(layout.refCkptKey(fixture::fixtureLife(ns)));

        ASSERT_TRUE(gc.runRegularRound().acquired_lease);
        const RefHold hold = holdOf(*backend, layout, ns);
        EXPECT_EQ(hold.reason, HoldReason::GapBelowWitness);
        EXPECT_EQ(hold.offending_position, (RefTxnId{1, 5}));
        EXPECT_EQ(sealedCursorOf(*backend, layout, ns), (RefTxnId{1, 4}));
    }
}

/// The second witness can also be UNREADABLE, and that is a different answer from absent. An absent
/// `_ckpt` says "this namespace published no checkpoint" and honestly contributes no witness; a present
/// one that will not decode says "this namespace HAS a checkpoint and we cannot read it", which no walk
/// may treat as no witness.
///
/// It is still ONE NAMESPACE'S object. The fold used to fail the whole round closed on it — every
/// namespace's cursor, seal and cleanup stopped, every round, on one unreadable 4 KiB object, and the
/// exception named neither the namespace nor the key. The rule is the one §5 states for every other
/// per-namespace failure: hold the namespace that owns the object, fold everything else.
TEST(CASGCHoldGrammar, AnUndecodableCheckpointHoldsOnlyItsOwnNamespace)
{
    const RootNamespace bad{"00/aa@cas@"};
    /// Stage B (Task 4-C): no pin needed -- `publishAt(..., birth=true)` below (draining into
    /// `writeRefLogTxnRaw`) admits `bad` into the catalog itself, pinned to the same sentinel this
    /// test's own `fixture::fixtureLife(bad)` key computations already assume.
    const RootNamespace good{"00/bb@cas@"};

    auto backend = std::make_shared<HintHoleCountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    Gc gc(store, kGc);

    publishAt(*backend, layout, bad, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, bad, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    publishAt(*backend, layout, good, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(11), /*birth=*/true);
    writeCkptAt(*backend, layout, bad, RefTxnId{1, 2});
    writeCkptAt(*backend, layout, good, RefTxnId{1, 1});

    /// Round 1 is the BASELINE both namespaces are measured against: each folds its whole stream and
    /// seals a cursor, and neither holds.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    ASSERT_EQ(sealedCursorOf(*backend, layout, bad), (RefTxnId{1, 2}));
    ASSERT_EQ(sealedCursorOf(*backend, layout, good), (RefTxnId{1, 1}));
    ASSERT_FALSE(coverageOf(*backend, layout, bad)->hold.has_value());

    /// Corrupt EXACTLY ONE OBJECT: the first namespace's `_ckpt` body. Nothing else in the pool changes,
    /// so everything the next round does differently is attributable to this one object.
    const String bad_ckpt_key = layout.refCkptKey(fixture::fixtureLife(bad));
    const HeadResult ckpt_head = backend->head(bad_ckpt_key);
    ASSERT_TRUE(ckpt_head.exists);
    ASSERT_EQ(backend->putOverwrite(bad_ckpt_key, "this is not a cas_ref_ckpt", ckpt_head.token).outcome,
              PutOutcome::Done);

    /// Work only a round that COMPLETES can fold.
    publishAt(*backend, layout, good, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(12));
    const String good_ckpt_key = layout.refCkptKey(fixture::fixtureLife(good));
    const HeadResult good_ckpt_head = backend->head(good_ckpt_key);
    ASSERT_TRUE(good_ckpt_head.exists);
    ASSERT_EQ(backend->putOverwrite(good_ckpt_key, encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 2},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = std::nullopt,
    }), good_ckpt_head.token).outcome, PutOutcome::Done);

    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    /// The namespace that owns the object is held, at the position its walk would have read next, and
    /// its coverage row rides UNCHANGED — the cursor may not move while the hold stands.
    const RefHold hold = holdOf(*backend, layout, bad);
    EXPECT_EQ(hold.reason, HoldReason::CheckpointUndecodable);
    EXPECT_EQ(hold.offending_position, (RefTxnId{1, 3}));
    EXPECT_EQ(sealedCursorOf(*backend, layout, bad), (RefTxnId{1, 2}));

    /// The other namespace folded its new record. This is the whole point of the finding: one corrupt
    /// object must not stop the pool.
    const auto good_cov = coverageOf(*backend, layout, good);
    ASSERT_TRUE(good_cov.has_value());
    EXPECT_FALSE(good_cov->hold.has_value()) << "the corrupt object belongs to the OTHER namespace";
    EXPECT_EQ(good_cov->last_folded_ref_id, (RefTxnId{1, 2}));
    EXPECT_EQ(good_cov->classification, 2);

    /// And nothing was destroyed for the held namespace: a hold shuts the round's destructive gate, so
    /// its ref objects — including the ones a cleanup range computed WITHOUT the unreadable checkpoint
    /// would have widened onto — are all still there.
    for (const RefTxnId & id : {RefTxnId{1, 1}, RefTxnId{1, 2}})
        EXPECT_TRUE(backend->head(layout.refLogKey(fixture::fixtureLife(bad), id)).exists)
            << "ref log " << renderRefTxnId(id) << " of the held namespace was deleted";
}

/// THE OTHER ARM OF THE SAME RULE, and the one that must NOT mint a hold.
///
/// A namespace can carry an undecodable `_ckpt` and offer the walk NO POSITION TO READ: never folded
/// (no sealed cursor) and no listed log. Two ways to get there, both real. A writer publishes the
/// object around its namespace's birth, so a `_ckpt` that lands before the birth log is durable is
/// exactly this shape. And `parseRefCkptKey` deliberately resolves anything of the form
/// `<something>/_ckpt`, so a key with a stray segment names the checkpoint of a table that has no logs
/// and no snapshots and never will (`CasLayout.h`, "the phantom table it names ... the fold does
/// nothing for it") — which is precisely the object that used to halt GC for the entire pool.
///
/// NO HOLD IS MINTED, and that is a positive design choice rather than a shortfall. A hold is not just
/// a stop flag: its `offending_position` is read by every later round as a DURABLE WITNESS that some
/// round once reached that position, which turns an absent below it into a gap rather than a frontier.
/// The walk here reached nothing, so any position would be invented — `{0, 0}` is rejected outright by
/// both codecs, and any canonical value would plant a permanent false witness under a namespace whose
/// records legitimately do not exist. The anomaly carries it instead, which is enough because it shuts
/// the same round-wide destructive gate a hold would, and because everything the checkpoint gates is a
/// no-op for a namespace the walk cannot even start on: nothing to fold, and an empty delete plan.
TEST(CASGCHoldGrammar, AnUndecodableCheckpointWithNoWalkPositionRecordsAnAnomalyAndMintsNoHold)
{
    const RootNamespace phantom{"00/aa@cas@"};
    const RootNamespace good{"00/bb@cas@"};

    auto backend = std::make_shared<HintHoleCountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    Gc gc(store, kGc);

    /// Stage B (Task 4-C): `phantom` gets no birth and no other production touch -- it is meant to
    /// have no logs and no snapshots, ever. But `discoverUniverse` is now catalog-authoritative, so a
    /// namespace absent from the catalog is invisible to the walk (R10 treats it as foreign-prefix-inert),
    /// and this test's whole premise -- that GC still surfaces an anomaly for an uncataloged `_ckpt` --
    /// would be silently defeated. Admitting it here (still with no `_ckpt` of its own) is what keeps
    /// `phantom` reachable by `readCheckpointWitnesses` without giving it the birth this test deliberately
    /// withholds.
    fixture::admitLive(*backend, layout, phantom);

    /// A lone `_ckpt` with an undecodable body, and NOTHING else under that namespace.
    backend->putIfAbsent(layout.refCkptKey(fixture::fixtureLife(phantom)), "this is not a cas_ref_ckpt");
    publishAt(*backend, layout, good, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(11), /*birth=*/true);
    writeCommittedCkptAt(*backend, layout, good, RefTxnId{1, 1});

    const RoundReport report = gc.runRegularRound();
    ASSERT_TRUE(report.acquired_lease);

    /// The anomaly is the whole carrier here: it is what shuts the round's destructive gate, and the
    /// gate is what keeps `cleanupRefObjects` from computing this namespace's delete range from an
    /// ABSENT checkpoint — which is the WIDEST reading, not the safest.
    /// Located by namespace and shard, then CHECKED ON ITS REASON. Asserting only that "some anomaly
    /// exists for this namespace" is a pin any future unrelated anomaly would satisfy, and this test
    /// would then stop testing anything; the reason check is what keeps it pinned to this arm. Note it
    /// is also stricter than searching BY reason would be — it requires the FIRST anomaly recorded for
    /// this namespace to be this one, not merely that one of them somewhere is.
    const auto anomaly = std::find_if(report.anomalies.begin(), report.anomalies.end(),
        [&](const RoundAnomaly & a) { return a.ns.string() == phantom.string() && a.shard == 0; });
    ASSERT_NE(anomaly, report.anomalies.end())
        << "an unreadable `_ckpt` must be surfaced even when there is no walk to stop";
    EXPECT_NE(anomaly->reason.find("_ckpt"), String::npos)
        << "the anomaly must say WHAT stopped the namespace, not merely that something did";

    const auto cov = coverageOf(*backend, layout, phantom);
    ASSERT_TRUE(cov.has_value());
    EXPECT_FALSE(cov->hold.has_value()) << "a hold here could only name a position no round ever read";
    EXPECT_EQ(cov->classification, 1) << "nothing was folded, so the row is `unchanged`";
    EXPECT_EQ(cov->last_folded_ref_id, (RefTxnId{}));

    /// Same isolation as the held arm: the pool keeps working.
    const auto good_cov = coverageOf(*backend, layout, good);
    ASSERT_TRUE(good_cov.has_value());
    EXPECT_FALSE(good_cov->hold.has_value());
    EXPECT_EQ(good_cov->last_folded_ref_id, (RefTxnId{1, 1}));

    /// The unreadable object itself is never deleted as debris — repairing it is the operator's move,
    /// and GC removing it would erase the only evidence of what stopped the namespace.
    EXPECT_TRUE(backend->head(layout.refCkptKey(fixture::fixtureLife(phantom))).exists);
}

/// ===================== THE HOLD IS DURABLE =====================

namespace
{

/// Seed a namespace held at {1,3} by a gap below the listed witness {1,4}, then make the hint forget
/// the namespace exists. Returns the round-1 hold.
RefHold seedHeldThenUnhinted(
    const std::shared_ptr<HintHoleCountingBackend> & backend, const PoolPtr & store,
    const RootNamespace & ns, Gc & gc)
{
    const Layout & layout = store->layout();
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    publishAt(*backend, layout, ns, RefTxnId{1, 4}, "ref_4", 4, DB::UInt128(4));
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 4});

    EXPECT_TRUE(gc.runRegularRound().acquired_lease);
    const RefHold hold = holdOf(*backend, layout, ns);

    /// Every one of the namespace's objects vanishes from every LIST while staying readable by key:
    /// the round that follows has no hint entry for this namespace at all.
    for (const RefTxnId & id : {RefTxnId{1, 1}, RefTxnId{1, 2}, RefTxnId{1, 4}})
        backend->hide(layout.refLogKey(fixture::fixtureLife(ns), id));
    return hold;
}

}

TEST(CASGCHoldGrammar, HoldRidesARoundWhoseHintOmitsTheNamespace)
{
    auto backend = std::make_shared<HintHoleCountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const RootNamespace ns{"00/aa@cas@"};
    Gc gc(store, kGc);
    const RefHold first = seedHeldThenUnhinted(backend, store, ns, gc);

    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    ASSERT_GT(backend->holesServed(), 0u);

    const RefHold second = holdOf(*backend, store->layout(), ns);
    EXPECT_EQ(second.reason, first.reason) << "a quiet hint must not rewrite why the namespace is held";
    EXPECT_EQ(second.offending_position, first.offending_position);
    EXPECT_EQ(sealedCursorOf(*backend, store->layout(), ns), (RefTxnId{1, 2}))
        << "the cursor may not advance while the hold stands";
    /// The one field that moves, and the reason it exists: it counts the rounds that retried and failed.
    EXPECT_EQ(second.retry_count, first.retry_count + 1);
}

TEST(CASGCHoldGrammar, HoldForcesAnExactRetryOfItsOffendingPositionWhenUnhinted)
{
    auto backend = std::make_shared<HintHoleCountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    Gc gc(store, kGc);
    seedHeldThenUnhinted(backend, store, ns, gc);

    const String offending = store->layout().refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3});
    const uint64_t before = backend->getCount(offending);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    EXPECT_GT(backend->getCount(offending), before)
        << "a carried hold must read its offending position by EXACT key; the hint cannot be asked, "
           "because the hint no longer mentions the namespace at all";
}

/// The clearing rule, stated as a test: an absent proves nothing. The round below observes the
/// offending position absent AGAIN, with no witness anywhere — exactly the observation a lying store
/// produces — and the hold survives it. Only the record actually appearing, being folded, and the
/// result reaching `gc/state` clears it.
TEST(CASGCHoldGrammar, HoldClearsOnlyByFoldingThroughTheOffendingPosition)
{
    auto backend = std::make_shared<HintHoleCountingBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);   /// Stage B (Task 4-C): pin to the sentinel before the first real touch
    Gc gc(store, kGc);
    seedHeldThenUnhinted(backend, store, ns, gc);

    /// Round 2: another absent, no witness. NOT a clearance.
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    EXPECT_EQ(holdOf(*backend, layout, ns).offending_position, (RefTxnId{1, 3}));

    /// The record appears at last (still invisible to every LIST — the hold is the only thing that
    /// knows to look there).
    publishAt(*backend, layout, ns, RefTxnId{1, 3}, "ref_3", 3, DB::UInt128(3));
    backend->hide(layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3}));

    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const auto cov = coverageOf(*backend, layout, ns);
    ASSERT_TRUE(cov.has_value());
    EXPECT_FALSE(cov->hold.has_value()) << "folding through the offending position is what clears a hold";
    EXPECT_EQ(cov->classification, 2);
    EXPECT_EQ(cov->last_folded_ref_id, (RefTxnId{1, 4})) << "the walk resumed past the resolved gap";
    EXPECT_EQ(inDegreeOf(*backend, layout, DB::UInt128(4)), 1)
        << "the record above the gap finally contributed its owner edge";
}

/// ===================== REBUILD =====================

namespace
{

/// Rewrite the fold seal at an EXACT `(generation, attempt)`, applying `mutate` to it. Needed where
/// the seal under test is not the adopted one — a step-down test plants its hold in a generation the
/// pool has already moved past.
void mutateSealAt(Backend & backend, const Layout & layout, uint64_t generation, uint64_t attempt,
                  const std::function<void(CasFoldSeal &)> & mutate)
{
    const String key = layout.foldSealKey(generation, attempt);
    CasFoldSeal seal = decodeFoldSeal(backend.get(key)->bytes);
    mutate(seal);
    backend.putOverwrite(key, encodeFoldSeal(seal), backend.head(key).token);
}

/// Rewrite the adopted fold seal, applying `mutate` to it. Used to plant a hold that the rebuild must
/// then carry: planting it directly (rather than by holding a real round) keeps the REBUILD tests about
/// the carry, not about how the hold arose.
void mutateAdoptedSeal(Backend & backend, const Layout & layout, const std::function<void(CasFoldSeal &)> & mutate)
{
    const GcState st = decodeGcState(backend.get(layout.gcStateKey())->bytes);
    const String key = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    CasFoldSeal seal = decodeFoldSeal(backend.get(key)->bytes);
    mutate(seal);
    backend.putOverwrite(key, encodeFoldSeal(seal), backend.head(key).token);
}

RefHold plantedHold()
{
    return RefHold{.reason = HoldReason::WitnessDisappeared, .offending_position = RefTxnId{4, 9},
                   .retry_count = 17, .next_retry_round = 23};
}

}

/// A rebuild carries a hold only for the matching catalog life. A historical row whose id is absent
/// from the rebuild cut is dropped and cannot mint output work.
TEST(CASGCHoldGrammar, RebuildCarriesMatchingHoldAndDropsAbsentLife)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 1});
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const UInt128 life_id = catalogLifeIdForTest(*backend, layout, ns);
    constexpr UInt128 absent_life_id{0xfeed};
    mutateAdoptedSeal(*backend, layout, [&](CasFoldSeal & seal)
    {
        RefCoverage & cov = seal.ref_lives.at(life_id).coverage;
        cov.classification = 4;
        cov.hold = plantedHold();
        RefCoverage gone;
        gone.classification = 4;
        gone.last_folded_ref_id = RefTxnId{2, 2};
        gone.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{2, 3},
                            .retry_count = 1, .next_retry_round = 2};
        seal.ref_lives[absent_life_id].coverage = gone;
    });

    const RebuildReport rep = gc.rebuildBaseline(/*force=*/true);
    ASSERT_TRUE(rep.performed) << rep.refusal;

    const auto rebuilt = newestSeal(*backend, layout);
    ASSERT_TRUE(rebuilt.has_value());
    const auto rediscovered = rebuilt->ref_lives.find(life_id);
    ASSERT_NE(rediscovered, rebuilt->ref_lives.end());
    EXPECT_EQ(rediscovered->second.coverage.classification, 4);
    ASSERT_TRUE(rediscovered->second.coverage.hold.has_value());
    EXPECT_EQ(*rediscovered->second.coverage.hold, plantedHold());
    EXPECT_FALSE(rebuilt->ref_lives.contains(absent_life_id));
}

/// AN ORDINARY CRASH IS NOT A CORRUPT POOL. A round writes its runs during the reduce phase and its
/// fold seal only at phase 10/18, so a crash in between leaves the newest generation existing WITHOUT
/// a seal — the commonest shape there is. If discovery stopped at the listing's maximum it would find
/// no seal there, conclude it could enumerate nothing, and refuse — telling the operator to recreate a
/// pool whose holds are sitting readable one generation down.
///
/// So discovery steps DOWN through the generations the listing itself reported until one carries a
/// seal. That spends no trust the maximum had not already been given. What it does NOT weaken is the
/// refusal above the maximum: that one stays terminal, because a seal found there is the listing
/// caught lying, not merely being incomplete about seals.
TEST(CASGCHoldGrammar, RebuildStepsDownPastACrashedNewestGenerationToTheSealBelowIt)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 1});
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const GcState after_first = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const uint64_t older_generation = after_first.snap_generation;
    const uint64_t older_attempt = after_first.snap_attempt;
    const UInt128 life_id = catalogLifeIdForTest(*backend, layout, ns);

    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    advanceRecoverableCkptForRawFixture(*backend, layout, ns, RefTxnId{1, 2});
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const GcState after_second = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(after_second.snap_generation, older_generation) << "the fixture needs two generations";

    /// The older generation is the one holding the pool's durable hold.
    mutateSealAt(*backend, layout, older_generation, older_attempt, [&](CasFoldSeal & seal)
    {
        RefCoverage & cov = seal.ref_lives.at(life_id).coverage;
        cov.classification = 4;
        cov.hold = plantedHold();
    });

    /// THE CRASH: the newest generation's run objects are there, its seal never got written. Then
    /// `gc/state` is lost, which is this path's whole premise.
    const String newest_seal = layout.foldSealKey(after_second.snap_generation, after_second.snap_attempt);
    const HeadResult seal_head = backend->head(newest_seal);
    ASSERT_TRUE(seal_head.exists);
    ASSERT_EQ(backend->deleteExact(newest_seal, seal_head.token).kind, DeleteOutcome::Kind::Deleted);
    ASSERT_FALSE(backend->list(layout.gcGenPrefix(after_second.snap_generation), "", 1).keys.empty())
        << "the crashed generation must still hold objects, or it is not the shape being modelled";
    const HeadResult sh = backend->head(layout.gcStateKey());
    ASSERT_EQ(backend->deleteExact(layout.gcStateKey(), sh.token).kind, DeleteOutcome::Kind::Deleted);

    Gc gc2(store, hexToU128("0000000000000000000000000000000c"));
    const RebuildReport rep = gc2.rebuildBaseline(/*force=*/false);
    ASSERT_TRUE(rep.performed) << rep.refusal;
    EXPECT_FALSE(rep.virgin_by_enumeration) << "a pool with a readable seal is not virgin";
    EXPECT_EQ(rep.adopted_seal_generation, older_generation)
        << "the report must name WHICH generation the holds came from, so a step-down is visible";

    const auto rebuilt = newestSeal(*backend, layout);
    ASSERT_TRUE(rebuilt.has_value());
    const auto it = rebuilt->ref_lives.find(life_id);
    ASSERT_NE(it, rebuilt->ref_lives.end());
    ASSERT_TRUE(it->second.coverage.hold.has_value())
        << "a crash between the run writes and the seal write turned into 'recreate the pool', and the "
           "hold readable one generation down was thrown away with it";
    EXPECT_EQ(*it->second.coverage.hold, plantedHold());
}

/// With no readable prior seal there is nothing to carry, and the holds it may have contained are
/// unknowable. The rebuild refuses rather than blessing a baseline whose provenance it cannot state —
/// a pool-wide hold is not representable (there is no offending position anyone could ever fold
/// through), so the honest answer is the refusal, and the recovery path is pool recreation.
TEST(CASGCHoldGrammar, RebuildRefusesWithAMissingPriorSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const GcState st = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(st.snap_generation, 0u);
    const String seal_key = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    const HeadResult sh = backend->head(seal_key);
    ASSERT_TRUE(sh.exists);
    ASSERT_EQ(backend->deleteExact(seal_key, sh.token).kind, DeleteOutcome::Kind::Deleted);

    /// FORCE does not buy past it either: force means "rebuild deliberately", never "drop the holds".
    for (const bool force : {false, true})
    {
        SCOPED_TRACE(force ? "force" : "plain");
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc.rebuildBaseline(force); });
    }

    const GcState after = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    EXPECT_EQ(after.snap_generation, st.snap_generation) << "a refused rebuild adopts nothing";
}

TEST(CASGCHoldGrammar, RebuildRefusesWithAnUndecodablePriorSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const GcState st = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const String seal_key = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    backend->putOverwrite(seal_key, "{\"type\":\"cas_fold_seal\",\"v\":4}\nthis is not a seal body\n",
                          backend->head(seal_key).token);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc.rebuildBaseline(/*force=*/true); });
}

/// LOSING THE POINTER IS NOT WEAKER THAN LOSING THE SEAL. `gc/state` names the adopted seal, and it is
/// the seal that carries the holds — so if the refusal only covered an unreadable seal, the *lesser*
/// corruption (the pointer is gone, every seal intact) would be treated more permissively than the
/// greater one, and the rebuild would write a baseline with no hold in it at all.
///
/// That matters because holds are not re-derivable by the next walk. `WitnessDisappeared` names a
/// record that is *gone*: the next round reads a clean frontier and would hand the namespace exactly
/// the frontier proof the hold exists to deny. Same for any hold whose only witness was the checkpoint
/// or the hold itself.
///
/// So with no adopted baseline named, the rebuild finds the newest fold seal OBJECT by enumeration and
/// carries its holds. This keeps the pool's disaster recovery intact — losing `gc/state` on a
/// lived-in pool is the scenario `REBUILD` exists for — while making it impossible to write a
/// hold-free baseline over a pool that had holds.
TEST(CASGCHoldGrammar, RebuildWithLostStateStillCarriesHoldsFromTheNewestSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 1});
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    const UInt128 life_id = catalogLifeIdForTest(*backend, layout, ns);
    mutateAdoptedSeal(*backend, layout, [&](CasFoldSeal & seal)
    {
        RefCoverage & cov = seal.ref_lives.at(life_id).coverage;
        cov.classification = 4;
        cov.hold = plantedHold();
    });

    /// The pointer vanishes; every seal object survives.
    const HeadResult sh = backend->head(layout.gcStateKey());
    ASSERT_TRUE(sh.exists);
    ASSERT_EQ(backend->deleteExact(layout.gcStateKey(), sh.token).kind, DeleteOutcome::Kind::Deleted);

    Gc gc2(store, hexToU128("00000000000000000000000000000009"));
    const RebuildReport rep = gc2.rebuildBaseline(/*force=*/false);
    ASSERT_TRUE(rep.performed) << rep.refusal;

    const auto rebuilt = newestSeal(*backend, layout);
    ASSERT_TRUE(rebuilt.has_value());
    const auto it = rebuilt->ref_lives.find(life_id);
    ASSERT_NE(it, rebuilt->ref_lives.end());
    ASSERT_TRUE(it->second.coverage.hold.has_value())
        << "the rebuild blessed a baseline with no hold in it, having read no seal at all";
    EXPECT_EQ(*it->second.coverage.hold, plantedHold());
}

/// ...and when that newest seal cannot be read either, there is nothing left to carry and no way to
/// know what was lost, so the rebuild refuses exactly as it does for an unreadable adopted seal.
TEST(CASGCHoldGrammar, RebuildRefusesWhenTheNewestSealIsUnreadableAndTheStateIsLost)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const GcState st = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    const String seal_key = layout.foldSealKey(st.snap_generation, st.snap_attempt);
    backend->putOverwrite(seal_key, "{\"type\":\"cas_fold_seal\",\"v\":4}\nthis is not a seal body\n",
                          backend->head(seal_key).token);
    const HeadResult sh = backend->head(layout.gcStateKey());
    ASSERT_EQ(backend->deleteExact(layout.gcStateKey(), sh.token).kind, DeleteOutcome::Kind::Deleted);

    Gc gc2(store, hexToU128("0000000000000000000000000000000a"));
    for (const bool force : {false, true})
    {
        SCOPED_TRACE(force ? "force" : "plain");
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc2.rebuildBaseline(force); });
    }
}

/// NEWEST-NESS IS NOT READ OFF A LISTING. Taking the newest seal from the pool-wide enumeration would
/// put the same hole one layer up: a listing that omits the true newest seal hands back an OLDER one,
/// and every hold detected since that older seal is silently lost. Two narrow single-generation probes
/// above the listing's maximum ask whether it lied.
///
/// And when it did lie, the answer is REFUSAL, not adoption of the newer seal. A store that misreports
/// its own enumeration DURING DISASTER RECOVERY does not get a second guess: adopting whatever the
/// second query happened to return would move the same trust one query along and prove nothing.
///
/// The fixture is the production shape rather than a contrivance: the broad `gc/gen/` enumeration
/// omits the newest generation's objects while a listing scoped to that generation still returns
/// them — the same class of lie the arithmetic ref walk was built for one layer down.
TEST(CASGCHoldGrammar, RebuildRefusesWhenANarrowProbeFindsASealAboveTheListingMaximum)
{
    /// Omits keys from ONE enumeration prefix only. Every other query — including a listing scoped to
    /// the generation itself — answers truthfully.
    class BroadListHoleBackend : public InMemoryBackend
    {
    public:
        String hide_under_prefix;
        String hidden_key_infix;
        size_t holes_served = 0;

        ListPage list(const String & prefix, const String & cursor, size_t limit) override
        {
            ListPage page = InMemoryBackend::list(prefix, cursor, limit);
            if (prefix != hide_under_prefix)
                return page;
            const size_t before = page.keys.size();
            std::erase_if(page.keys,
                          [&](const ListedKey & k) { return k.key.find(hidden_key_infix) != String::npos; });
            if (page.keys.size() != before)
                ++holes_served;
            return page;
        }
    };

    auto backend = std::make_shared<BroadListHoleBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    Gc gc(store, kGc);
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);
    publishAt(*backend, layout, ns, RefTxnId{1, 2}, "ref_2", 2, DB::UInt128(2));
    ASSERT_TRUE(gc.runRegularRound().acquired_lease);

    const GcState st = decodeGcState(backend->get(layout.gcStateKey())->bytes);
    ASSERT_GT(st.snap_generation, 1u) << "the fixture needs a newer generation to hide";
    const UInt128 life_id = catalogLifeIdForTest(*backend, layout, ns);
    mutateAdoptedSeal(*backend, layout, [&](CasFoldSeal & seal)
    {
        RefCoverage & cov = seal.ref_lives.at(life_id).coverage;
        cov.classification = 4;
        cov.hold = plantedHold();
    });

    /// The pool-wide enumeration loses the newest generation entirely; the pointer to it is deleted.
    const String gen_prefix = layout.gcGenPrefix(0);
    backend->hide_under_prefix = gen_prefix.substr(0, gen_prefix.size() - 2);   /// ".../gc/gen/"
    backend->hidden_key_infix = layout.gcGenPrefix(st.snap_generation);
    const HeadResult sh = backend->head(layout.gcStateKey());
    ASSERT_EQ(backend->deleteExact(layout.gcStateKey(), sh.token).kind, DeleteOutcome::Kind::Deleted);

    Gc gc2(store, hexToU128("0000000000000000000000000000000b"));
    for (const bool force : {false, true})
    {
        SCOPED_TRACE(force ? "force" : "plain");
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { gc2.rebuildBaseline(force); });
    }
    ASSERT_GT(backend->holes_served, 0u) << "the broad listing never actually lied";

    /// Nothing was adopted: the refusal fires before the lease, so the pool is exactly as it was.
    EXPECT_FALSE(backend->head(layout.gcStateKey()).exists)
        << "a refused rebuild must not mint a baseline, nor a bootstrap body";
}

/// The virgin verdict, pinned so the refusal can never grow to swallow a fresh pool — and pinned as
/// what it actually is. It rests on THREE pieces of enumeration evidence (wide LIST empty, narrow
/// generation-1 probe empty, no `gc/state`) and on no point read at all, so it is COUNTED: an operator
/// reading a disaster-recovery run needs to see that the clean slate came from enumeration rather than
/// from proof. `CASGCRebuildVirginByEnumeration` on a pool that has ever completed a round means the
/// enumeration lied.
TEST(CASGCHoldGrammar, RebuildProceedsOnAPoolThatNeverSealedABaselineAndCountsTheVerdict)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend, /*gc_fold_max_defer_rounds=*/0);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/aa@cas@"};

    /// No round has run, so there is no `gc/state` and no seal — only owner state to rebuild from.
    publishAt(*backend, layout, ns, RefTxnId{1, 1}, "ref_1", 1, DB::UInt128(1), /*birth=*/true);
    writeCommittedCkptAt(*backend, layout, ns, RefTxnId{1, 1});
    ASSERT_FALSE(backend->head(layout.gcStateKey()).exists);

    using ProfileEvents::global_counters;
    const auto virgin_before = global_counters[ProfileEvents::CASGCRebuildVirginByEnumeration].load();

    Gc gc(store, kGc);
    const RebuildReport rep = gc.rebuildBaseline(/*force=*/false);
    EXPECT_TRUE(rep.performed) << rep.refusal;
    EXPECT_GT(global_counters[ProfileEvents::CASGCRebuildVirginByEnumeration].load(), virgin_before)
        << "a clean slate granted from enumeration alone must be visible to whoever reads the run";
}
