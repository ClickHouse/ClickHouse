#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; extern const int NOT_IMPLEMENTED; }

using namespace DB::Cas;

namespace
{
UInt128 b(uint64_t n) { return UInt128(n); }
UInt128 s(uint64_t n) { return UInt128(n); }   // source-edge id
/// A `BlobRef` (CityHash128) for the same literal `n` — every existing test's `BlobDelta.ref` /
/// `BlobCandidate.ref` / `inDegreeInRuns` argument is a `BlobRef` as of Phase 3 T3.
BlobRef bh(uint64_t n) { return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(n))}; }

/// Scale thresholds for the "the run genuinely spans several blocks" sanity assertions below. These are
/// NOT format constants — the SourceEdge run is a plain NDJSON stream (`CasRecordStreamFormat`) with no
/// block framing of its own — they only pin the same byte-size scale the (now-deleted, codecs-v3 phase 6)
/// `CasRunFile` block codec used, so the multi-block-sized fixtures below stay meaningfully large.
/// (Previously read straight off `CasRunFile.h`'s own `kRunTargetBlockSize`/`kRunHardCapBlockSize`; this
/// file's `#include` of that header looked removable when `CasRunFile` was deleted in the phase-6 cutover,
/// but these two thresholds turned out to be the only remaining users — hence the local, explicitly-legacy
/// copies here instead of a dangling include. Values unchanged.)
constexpr uint32_t kLegacyBlockSize = 256u * 1024u;
constexpr uint32_t kLegacyHardCapBlockSize = 1024u * 1024u;
}

TEST(CASBlobInDegree, FoldStartsFromEmptyPriorGeneration)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Generation 1 from empty prior: two distinct edges on b1 and one on b2.
    /// Edge (b1,s1), (b1,s2), (b2,s1) => indeg(b1)=2, indeg(b2)=1.
    std::vector<BlobDelta> deltas{
        {bh(1), s(1), false},
        {bh(1), s(2), false},
        {bh(2), s(1), false},
    };
    std::vector<RunRef> runs;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, /*new*/1, /*attempt*/0, /*shard*/0, deltas, runs);
    ASSERT_FALSE(runs.empty());

    const auto zero = zeroInDegree(backend, runs);
    EXPECT_TRUE(zero.empty());   /// nothing at zero yet
}

TEST(CASBlobInDegree, PlusMinusCancelToZeroDetectsCandidate)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Gen 1: activate edge (b1,s1) and (b2,s1).
    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/0, 0,
        {{bh(1), s(1), false}, {bh(2), s(1), false}}, runs1);

    /// Generation 2 merges prior gen-1 run (resolved via runs1 refs) with removal of (b1,s1): indeg(b1)=0, indeg(b2)=1.
    std::vector<RunRef> runs2;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, /*new*/2, /*attempt*/0, 0,
        {{bh(1), s(1), true}}, runs2);

    const auto zero = zeroInDegree(backend, runs2);
    ASSERT_EQ(zero.size(), 1u);
    EXPECT_EQ(zero[0].ref, bh(1));
}

TEST(CASBlobInDegree, RunsAreByteDeterministic)
{
    InMemoryBackend a;
    InMemoryBackend b2;
    Layout layout{"pool"};
    std::vector<RunRef> ra;
    std::vector<RunRef> rb;
    /// Same deltas in a DIFFERENT input order must produce the same sealed run bytes (sorted by key).
    foldDeltasIntoGeneration(a,  layout, /*prior_runs*/{}, 1, /*attempt*/0, 0,
        {{bh(3), s(1), false}, {bh(1), s(1), false}, {bh(2), s(1), false}}, ra);
    foldDeltasIntoGeneration(b2, layout, /*prior_runs*/{}, 1, /*attempt*/0, 0,
        {{bh(1), s(1), false}, {bh(2), s(1), false}, {bh(3), s(1), false}}, rb);
    const auto ga = a.get(layout.blobTargetRunKey(1, /*attempt*/0, 0, 0));
    const auto gb = b2.get(layout.blobTargetRunKey(1, /*attempt*/0, 0, 0));
    ASSERT_TRUE(ga.has_value());
    ASSERT_TRUE(gb.has_value());
    EXPECT_EQ(ga->bytes, gb->bytes);
    ASSERT_EQ(ra.size(), 1u);
    ASSERT_EQ(rb.size(), 1u);
    EXPECT_EQ(ra[0].checksum, rb[0].checksum);
}

TEST(CASBlobInDegree, SameEdgeActivatedTwiceCountsOnce)
{
    /// Idempotency: activating the same (blob_hash, source_id) twice must not double-count.
    /// The source-edge set is a SET, not a counter — re-adding the same edge is a no-op.
    /// indeg(b1) must be 1 after both activations, not 2.
    InMemoryBackend backend;
    Layout layout{"pool"};
    std::vector<BlobDelta> deltas{
        {bh(1), s(1), false},   // activate (b1,s1)
        {bh(1), s(1), false},   // same edge again — must deduplicate
    };
    std::vector<RunRef> runs;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/0, 0, deltas, runs);
    ASSERT_FALSE(runs.empty());

    const int64_t deg = DB::Cas::tests::inDegreeInRuns(backend, runs, bh(1));
    EXPECT_EQ(deg, 1);   /// deduplicated, not 2

    const auto zero = zeroInDegree(backend, runs);
    EXPECT_TRUE(zero.empty());   /// b1 still has an active edge
}

TEST(CASBlobInDegree, FoldDeltaByteEqualReplayAdopts)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    std::vector<BlobDelta> deltas{{bh(1), s(1), false}};
    std::vector<RunRef> runs1;
    std::vector<RunRef> runs2;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/7, /*shard*/0, deltas, runs1);
    /// Same inputs, same attempt => byte-identical run already present => adopt, no throw.
    EXPECT_NO_THROW(foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/7, /*shard*/0, deltas, runs2));
    EXPECT_EQ(runs1, runs2);
}

TEST(CASBlobInDegree, FoldDeltaDivergentBytesThrowsCorrupted)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    /// Pre-occupy the run key (attempt 7) with junk, then fold => divergent => CORRUPTED_DATA.
    backend.putIfAbsent(layout.blobTargetRunKey(1, /*attempt*/7, /*shard*/0, /*seq*/0), "not-a-valid-run");
    std::vector<BlobDelta> deltas{{bh(1), s(1), false}};
    std::vector<RunRef> runs;
    EXPECT_THROW(foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/7, /*shard*/0, deltas, runs),
                 DB::Exception);
}

/// ==== two-cursor settlement merge (retired-in-snapshot T3, spec §2.1/§3) ====
///
/// The retired input is no longer a separate `prior_retired` vector — the prior generation's `kCondemned`
/// rows RIDE the source-edge run at the zero-sentinel key. These helpers build such a prior run directly
/// (via the sorted-NDJSON `SourceEdgeRunWriter`, codecs-v3 phase 5) and decode a run for assertions.

namespace
{

/// A `kCondemned` sentinel record for `h` at the zero source_id, carrying the condemned incarnation.
SourceEdgeRecord condemnedRec(UInt128 h, const CondemnedRow & row)
{
    return SourceEdgeRecord{.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)},
                            .source_id = UInt128{0}, .marker = kCondemned,
                            .delete_pending = row.delete_pending, .token = row.token,
                            .size = row.size, .condemn_round = row.condemn_round};
}

/// An active-edge record (`kEdgeActive`) for `h` at source `sid`.
SourceEdgeRecord edgeRec(UInt128 h, UInt128 sid)
{
    return SourceEdgeRecord{.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)},
                            .source_id = sid, .marker = kEdgeActive};
}

/// head_blob / peek_head stub: present with a fixed token/size.
std::function<std::optional<HeadResult>(const BlobRef &)> headPresent(const String & tok, uint64_t size)
{
    return [tok, size](const BlobRef &) -> std::optional<HeadResult>
    {
        HeadResult hr;
        hr.exists = true;
        hr.size = size;
        hr.token = Token{.value = tok, .type = TokenType::Emulated};
        return hr;
    };
}

/// A `CondemnedRow` mirroring the old `entry(hash, condemn_round)` fixture (token "t", size 1).
CondemnedRow condemnedRowFor(uint64_t condemn_round, const String & tok = "t",
                             bool delete_pending = false, uint64_t size = 1)
{
    return CondemnedRow{.delete_pending = delete_pending,
                        .token = Token{.value = tok, .type = TokenType::Emulated},
                        .size = size, .condemn_round = condemn_round};
}

/// Build a source-edge run (`kSourceEdgeKeySchema128`) carrying the given `kCondemned` sentinel rows
/// and surviving edges, write it under `blobTargetRunKey(gen, attempt, shard, 0)`, and return its
/// `RunRef`. Rows are emitted in (blob_hash, source_id) order (sentinels at source_id 0 sort first
/// per blob).
RunRef writeSourceEdgeRun(InMemoryBackend & backend, const Layout & layout,
                          uint64_t gen, uint64_t attempt, uint64_t shard,
                          const std::vector<std::pair<UInt128, CondemnedRow>> & condemned,
                          const std::vector<std::pair<UInt128, UInt128>> & edges = {})
{
    std::vector<SourceEdgeRecord> recs;
    for (const auto & [h, row] : condemned)
        recs.push_back(condemnedRec(h, row));
    for (const auto & [h, sid] : edges)
        recs.push_back(edgeRec(h, sid));
    /// The writer requires non-decreasing (ref, source_id) order (sentinels at source_id 0 sort first
    /// per blob, exactly reproducing the old raw-key order).
    std::stable_sort(recs.begin(), recs.end(), [](const SourceEdgeRecord & a, const SourceEdgeRecord & bb)
    {
        if (a.ref < bb.ref)
            return true;
        if (bb.ref < a.ref)
            return false;
        return a.source_id < bb.source_id;
    });

    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    for (const auto & rec : recs)
        writer.append(rec);
    writer.finish();
    out.finalize();

    const String bytes = out.str();
    const String key = layout.blobTargetRunKey(gen, attempt, shard, 0);
    backend.putIfAbsent(key, bytes);
    return RunRef{.key = key, .checksum = sourceEdgeRunChecksum(bytes), .shard = shard, .generation = gen};
}

struct DecodedRun
{
    std::vector<std::pair<UInt128, CondemnedRow>> condemned;   /// (blob_hash, row)
    std::vector<UInt128> zero_markers;                         /// blob hashes with a zero-transition marker
    std::vector<std::pair<UInt128, UInt128>> edges;            /// (blob_hash, source_id)
};

DecodedRun decodeRun(InMemoryBackend & backend, const RunRef & run)
{
    DecodedRun d;
    auto r = openSourceEdgeRun(backend, run.key);
    /// Every run this test helper decodes is CityHash128 (16-byte), so `.toU128()` is a
    /// provably-exact round trip.
    String k;
    String p;
    while (r.next(k, p))
    {
        BlobRef bh_ref;
        UInt128 sid;
        SourceEdgeKeyCodec::parse(k, bh_ref, sid);   // throws CORRUPTED_DATA on a malformed key (fail-closed)
        const UInt128 bh = bh_ref.digest.toU128();
        EXPECT_FALSE(p.empty());
        if (p.empty())
            continue;
        if (p[0] == kCondemned)
            d.condemned.emplace_back(bh, decodeCondemnedRow(p));
        else if (p[0] == kZeroMarker)
            d.zero_markers.push_back(bh);
        else if (p[0] == kEdgeActive)
            d.edges.emplace_back(bh, sid);
        else
            ADD_FAILURE() << "unknown run row type";
    }
    return d;
}

}

/// Per-consumer whole-file seal-checksum RED tests (codecs-v3 phase 5, Task 6): a run whose ROWS are
/// well-formed (so `cursor.advance()` never aborts first) but whose `RunRef.checksum` disagrees with the
/// stored bytes must fail closed at each deletion-deriving consumer BEFORE any decision is produced. The
/// stored bytes are the valid run; only the seal checksum handed to the consumer is wrong.
TEST(CASBlobInDegree, FoldSealChecksumMismatchFailsClosed)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    const RunRef good = writeSourceEdgeRun(backend, layout, /*gen*/1, /*attempt*/0, /*shard*/0,
                                           /*condemned*/{}, /*edges*/{{b(1), s(1)}});
    RunRef bad = good;
    bad.checksum = good.checksum + 1;   /// rows still parse; only the seal disagrees
    std::vector<RunRef> prior{bad};
    std::vector<RunRef> out;
    /// A delta on a DIFFERENT blob forces the two-cursor merge to stream the prior run to completion, so
    /// the end-of-segment verifyAgainst fires (not a row-invariant abort).
    EXPECT_THROW(
        foldDeltasIntoGeneration(backend, layout, prior, /*new*/2, /*attempt*/0, /*shard*/0,
                                 std::vector<BlobDelta>{{bh(2), s(1), false}}, out),
        DB::Exception);
}

TEST(CASBlobInDegree, ZeroInDegreeSealChecksumMismatchFailsClosed)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    const RunRef good = writeSourceEdgeRun(backend, layout, /*gen*/1, /*attempt*/0, /*shard*/0,
                                           /*condemned*/{}, /*edges*/{{b(1), s(1)}});
    RunRef bad = good;
    bad.checksum = good.checksum + 1;
    std::vector<RunRef> runs{bad};
    EXPECT_THROW(zeroInDegree(backend, runs), DB::Exception);
}

TEST(CASThreeCursorMerge, FloorBoundary)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Gen 1's run holds one unrelated surviving edge (b9) plus the carried kCondemned rows for A=b1
    /// (condemned round 2) and B=b2 (round 3); neither A nor B has any edge (in-degree 0 by definition).
    /// current_round = 3: strictly-below graduates, at-the-current-round stays.
    const RunRef gen1 = writeSourceEdgeRun(backend, layout, /*gen*/1, 0, 0,
        {{b(1), condemnedRowFor(2)}, {b(2), condemnedRowFor(3)}}, {{b(9), s(1)}});

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{gen1}, 2, 0, 0, {}, runs2,
        /*current_round*/3, /*condemn_round*/4, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    /// Two-phase graduation: the floor-passed entry is REPUBLISHED pending (still in the list);
    /// its physical delete belongs to the NEXT pass.
    ASSERT_EQ(rmr.graduated.size(), 1u);
    EXPECT_EQ(rmr.graduated[0].ref, bh(1));
    EXPECT_TRUE(rmr.graduated[0].delete_pending);
    ASSERT_EQ(rmr.still_retired.size(), 2u);
    EXPECT_EQ(rmr.still_retired[0].ref, bh(1));
    EXPECT_TRUE(rmr.still_retired[0].delete_pending);
    EXPECT_EQ(rmr.still_retired[1].ref, bh(2));
    EXPECT_FALSE(rmr.still_retired[1].delete_pending);
    EXPECT_EQ(rmr.still_retired[1].condemn_round, 3u);   /// carried unchanged, not re-stamped
    EXPECT_TRUE(rmr.spared.empty());
    EXPECT_TRUE(rmr.redelete.empty());

    /// still_retired mirrors exactly the kCondemned rows written into the output run, in order.
    const DecodedRun out = decodeRun(backend, runs2[0]);
    ASSERT_EQ(out.condemned.size(), 2u);
    EXPECT_EQ(out.condemned[0].first, b(1));
    EXPECT_TRUE(out.condemned[0].second.delete_pending);
    EXPECT_EQ(out.condemned[1].first, b(2));
    EXPECT_FALSE(out.condemned[1].second.delete_pending);
    EXPECT_TRUE(out.zero_markers.empty());
}

TEST(CASThreeCursorMerge, PendingRedeletesAndDrops)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// A row the PRIOR pass published as delete_pending (carried on gen 1's run): this pass hands it to
    /// `redelete` (executed pre-CAS by the caller) and drops it from the output run.
    const RunRef gen1 = writeSourceEdgeRun(backend, layout, /*gen*/1, 0, 0,
        {{b(1), condemnedRowFor(1, "t", /*delete_pending*/true)}});

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{gen1}, 2, 0, 0, {}, runs2,
        /*current_round*/9, /*condemn_round*/9, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    ASSERT_EQ(rmr.redelete.size(), 1u);
    EXPECT_EQ(rmr.redelete[0].ref, bh(1));
    EXPECT_TRUE(rmr.still_retired.empty());
    EXPECT_TRUE(rmr.graduated.empty());
    EXPECT_TRUE(rmr.spared.empty());

    /// The redeleted blob leaves the run entirely (no sentinel carried, no zero marker — untouched).
    const DecodedRun out = decodeRun(backend, runs2[0]);
    EXPECT_TRUE(out.condemned.empty());
    EXPECT_TRUE(out.zero_markers.empty());
}

TEST(CASThreeCursorMerge, RecoverySpares)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// A (=b1) is retired at round 1 and would long since have graduated (current_round = 5) — but this
    /// pass's delta adds an edge to it: recovery WINS over graduation, the entry is dropped as spared.
    const RunRef gen1 = writeSourceEdgeRun(backend, layout, /*gen*/1, 0, 0, {{b(1), condemnedRowFor(1)}});

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{gen1}, 2, 0, 0, {{bh(1), s(1), false}}, runs2,
        /*current_round*/5, /*condemn_round*/6, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    ASSERT_EQ(rmr.spared.size(), 1u);
    EXPECT_EQ(rmr.spared[0].ref, bh(1));
    EXPECT_TRUE(rmr.graduated.empty());
    EXPECT_TRUE(rmr.still_retired.empty());

    /// b1 recovered its edge: the output run carries the surviving edge and no sentinel for it.
    const DecodedRun out = decodeRun(backend, runs2[0]);
    EXPECT_TRUE(out.condemned.empty());
    ASSERT_EQ(out.edges.size(), 1u);
    EXPECT_EQ(out.edges[0].first, b(1));
}

TEST(CASThreeCursorMerge, NewCandidateCondemned)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Gen 1: C (=b3) has one edge. Gen 2 removes it => transition to zero, not retired =>
    /// condemned with the head-captured token at THIS pass's condemn_round.
    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, 0, 0, {{bh(3), s(1), false}}, runs1);

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, 2, 0, 0, {{bh(3), s(1), true}}, runs2,
        /*current_round*/0, /*condemn_round*/7, headPresent("t9", 42), /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    ASSERT_EQ(rmr.still_retired.size(), 1u);
    EXPECT_EQ(rmr.still_retired[0].ref, bh(3));
    EXPECT_EQ(rmr.still_retired[0].token.value, "t9");
    EXPECT_EQ(rmr.still_retired[0].size, 42u);
    EXPECT_EQ(rmr.still_retired[0].condemn_round, 7u);
    EXPECT_TRUE(rmr.graduated.empty());
    EXPECT_TRUE(rmr.spared.empty());

    /// The fresh condemn is emitted as a kCondemned row (not a zero marker) into the output run.
    const DecodedRun out = decodeRun(backend, runs2[0]);
    ASSERT_EQ(out.condemned.size(), 1u);
    EXPECT_EQ(out.condemned[0].first, b(3));
    EXPECT_EQ(out.condemned[0].second.token.value, "t9");
    EXPECT_TRUE(out.zero_markers.empty());
}

TEST(CASThreeCursorMerge, AbsentBlobNotCondemned)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Same transition-to-zero as above, but the blob object is already gone at condemn time:
    /// nothing to delete later, so no entry is minted — a plain zero marker is emitted instead.
    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, 0, 0, {{bh(3), s(1), false}}, runs1);

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, 2, 0, 0, {{bh(3), s(1), true}}, runs2,
        /*current_round*/0, /*condemn_round*/7,
        [](const BlobRef &) -> std::optional<HeadResult> { return std::nullopt; }, /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    EXPECT_TRUE(rmr.still_retired.empty());
    EXPECT_TRUE(rmr.graduated.empty());
    EXPECT_TRUE(rmr.spared.empty());

    const DecodedRun out = decodeRun(backend, runs2[0]);
    EXPECT_TRUE(out.condemned.empty());
    ASSERT_EQ(out.zero_markers.size(), 1u);
    EXPECT_EQ(out.zero_markers[0], b(3));
}

TEST(CASThreeCursorMerge, SnapshotEdgesUnperturbedByRetired)
{
    /// Retired-in-snapshot changes the byte-invariant: the retired machinery now WRITES kCondemned
    /// sentinel rows into the run, so a retired-engaged run is no longer byte-identical to a plain one.
    /// The preserved invariant (spec §2.1) is narrower: the retired machinery touches ONLY the sentinel
    /// namespace — the surviving EDGE rows are byte-identical to a plain fold of the same deltas.
    InMemoryBackend plain;
    InMemoryBackend engaged;
    Layout layout{"pool"};

    std::vector<RunRef> r1;
    foldDeltasIntoGeneration(plain, layout, /*prior_runs*/{}, 1, 0, 0,
        {{bh(1), s(1), false}, {bh(2), s(1), false}, {bh(2), s(2), true}}, r1);

    /// Engaged: the SAME deltas, but the prior run carries retired rows for b1 (which the delta re-edges
    /// => spared) and b5 (no edge => graduates past the floor).
    const RunRef prior = writeSourceEdgeRun(engaged, layout, /*gen*/1, 0, 0,
        {{b(1), condemnedRowFor(1)}, {b(5), condemnedRowFor(2)}});
    std::vector<RunRef> r2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(engaged, layout, /*prior_runs*/{prior}, 2, 0, 0,
        {{bh(1), s(1), false}, {bh(2), s(1), false}, {bh(2), s(2), true}}, r2,
        /*current_round*/9, /*condemn_round*/3, headPresent("t", 1), /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    const DecodedRun plain_run = decodeRun(plain, r1[0]);
    const DecodedRun engaged_run = decodeRun(engaged, r2[0]);
    EXPECT_EQ(plain_run.edges, engaged_run.edges);   /// edge rows byte-identical
    EXPECT_TRUE(plain_run.condemned.empty());
    /// The engaged run carries only the retired sentinel(s) on top: b1 spared (no row), b5 graduated.
    ASSERT_EQ(engaged_run.condemned.size(), 1u);
    EXPECT_EQ(engaged_run.condemned[0].first, b(5));
    EXPECT_TRUE(engaged_run.condemned[0].second.delete_pending);
}

TEST(CASTwoCursorMerge, CarriedSentinelIsNotATouch)
{
    /// Gen 1 condemns b (a real +edge/-edge net-to-zero with head_blob present) -> a kCondemned row. Gen 2
    /// has NO deltas at all: the carried row must (a) survive byte-identically, (b) emit no zero marker,
    /// (c) never call peek_head (a carried sentinel is not a touch).
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Gen 1: (b,s1) added then removed => net-to-zero => fresh condemn at round 5 (token "tok", size 7).
    std::vector<RunRef> runs1;
    RetiredMergeResult rmr1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, 0, 0,
        {{bh(2), s(1), false}, {bh(2), s(1), true}}, runs1,
        /*current_round*/0, /*condemn_round*/5, headPresent("tok", 7), /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr1);
    ASSERT_EQ(rmr1.still_retired.size(), 1u);
    {
        const DecodedRun g1 = decodeRun(backend, runs1[0]);
        ASSERT_EQ(g1.condemned.size(), 1u);
        EXPECT_EQ(g1.condemned[0].first, b(2));
        EXPECT_TRUE(g1.zero_markers.empty());   /// a condemned blob emits kCondemned, never a zero marker
    }

    /// Gen 2: empty deltas, current_round 1 (< 5 => b carries, does not graduate). peek_head must NOT fire.
    size_t peek_calls = 0;
    auto peek = [&](const BlobRef &) -> std::optional<HeadResult> { ++peek_calls; return {}; };
    std::vector<RunRef> runs2;
    RetiredMergeResult rmr2;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, 2, 0, 0, {}, runs2,
        /*current_round*/1, /*condemn_round*/6, /*head_blob*/{}, peek, /*confirm_condemned_marker*/{}, &rmr2);

    EXPECT_EQ(peek_calls, 0u);
    ASSERT_EQ(rmr2.still_retired.size(), 1u);
    EXPECT_EQ(rmr2.still_retired[0].ref, bh(2));
    EXPECT_EQ(rmr2.still_retired[0].condemn_round, 5u);   /// carried unchanged
    EXPECT_TRUE(rmr2.graduated.empty());

    const DecodedRun g2 = decodeRun(backend, runs2[0]);
    ASSERT_EQ(g2.condemned.size(), 1u);
    EXPECT_EQ(g2.condemned[0].first, b(2));
    EXPECT_EQ(g2.condemned[0].second.token.value, "tok");
    EXPECT_EQ(g2.condemned[0].second.size, 7u);
    EXPECT_TRUE(g2.zero_markers.empty());
}

TEST(CASTwoCursorMerge, MalformedRunFailsClosed)
{
    Layout layout{"pool"};

    /// (1) An active edge at the reserved sentinel source_id 0 -> the merge cursor fails closed.
    {
        InMemoryBackend backend;
        DB::WriteBufferFromOwnString out;
        SourceEdgeRunWriter writer(out);
        writer.append(edgeRec(1, UInt128{0}));   // edge at sentinel key
        writer.finish();
        out.finalize();
        const String bytes = out.str();
        const RunRef bad{.key = layout.blobTargetRunKey(1, 0, 0, 0),
                         .checksum = sourceEdgeRunChecksum(bytes), .shard = 0, .generation = 1};
        backend.putIfAbsent(bad.key, bytes);

        std::vector<RunRef> runs2;
        EXPECT_THROW(foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{bad}, 2, 0, 0, {}, runs2),
                     DB::Exception);
    }

    /// (2) Two sentinel rows for one blob -> duplicate sentinel -> the merge cursor fails closed.
    {
        InMemoryBackend backend;
        DB::WriteBufferFromOwnString out;
        SourceEdgeRunWriter writer(out);
        /// Same (b,0) key twice (equal keys are allowed by the writer) — two condemned sentinels for b1.
        writer.append(condemnedRec(1, condemnedRowFor(1)));
        writer.append(condemnedRec(1, condemnedRowFor(2)));
        writer.finish();
        out.finalize();
        const String bytes = out.str();
        const RunRef bad{.key = layout.blobTargetRunKey(1, 0, 0, 0),
                         .checksum = sourceEdgeRunChecksum(bytes), .shard = 0, .generation = 1};
        backend.putIfAbsent(bad.key, bytes);

        std::vector<RunRef> runs2;
        EXPECT_THROW(foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{bad}, 2, 0, 0, {}, runs2),
                     DB::Exception);
    }
}

/// A prior run spanning several blocks folds correctly with the streaming prior cursor AND the backend
/// sees only block-bounded ranged/stream requests for it — never a whole-object get of the prior run
/// key. Byte-reproducibility of the merged output is the load-bearing canary (the merge logic is
/// unchanged; only the prior cursor's byte source moved from materialize-whole to stream).
TEST(CASBlobInDegree, FoldStreamsPriorRunBlockBounded)
{
    using DB::Cas::tests::CountingBackend;
    CountingBackend backend;
    /// InMemory oracle: the SAME two folds against a plain backend must yield byte-identical runs —
    /// the streaming cursor changes I/O shape, not bytes.
    InMemoryBackend oracle;
    Layout layout{"pool"};

    /// Gen 1 from empty prior: enough edges that the SourceEdge run spills across many 256KB blocks.
    /// Each record is 4 + 32(key) + 4 + 1(payload) = 41 bytes, so ~20000 edges is ~820KB => several
    /// blocks under the default block_size, exercising the multi-block streaming path in the fold.
    std::vector<BlobDelta> gen1;
    gen1.reserve(20000);
    for (uint64_t i = 0; i < 20000; ++i)
        gen1.push_back({bh(i), s(1), false});

    std::vector<RunRef> runs1_c;
    std::vector<RunRef> runs1_o;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, 0, 0, gen1, runs1_c);
    foldDeltasIntoGeneration(oracle, layout, /*prior_runs*/{}, 1, 0, 0, gen1, runs1_o);

    const String gen1_run_key = layout.blobTargetRunKey(1, 0, 0, 0);
    const auto gen1_run = backend.get(gen1_run_key);
    ASSERT_TRUE(gen1_run.has_value());
    const String gen1_run_bytes = gen1_run->bytes;
    /// Sanity: the prior run really spans several blocks (else the block-bounded assertions are
    /// vacuous). Blocks seal at kLegacyBlockSize (256KB); ~820KB is 3-4 blocks.
    ASSERT_GT(gen1_run_bytes.size(), static_cast<size_t>(kLegacyBlockSize) * 3);

    /// Reset counters and fold gen 2 with a small delta: remove one edge and add another. The prior
    /// gen-1 run must be consumed via the streaming cursor (head + tail get + body getStream + per-seq
    /// head probe), NEVER a whole-object get.
    backend.resetCounts();
    std::vector<BlobDelta> gen2{{bh(0), s(1), true}, {bh(19999), s(2), false}};
    std::vector<RunRef> runs2_c;
    std::vector<RunRef> runs2_o;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1_c, 2, 0, 0, gen2, runs2_c);
    foldDeltasIntoGeneration(oracle, layout, /*prior_runs*/runs1_o, 2, 0, 0, gen2, runs2_o);

    /// Byte-reproducibility canary: streaming and materialized folds produce identical output bytes.
    const String gen2_run_key = layout.blobTargetRunKey(2, 0, 0, 0);
    const auto gen2_c = backend.get(gen2_run_key);
    const auto gen2_o = oracle.get(gen2_run_key);
    ASSERT_TRUE(gen2_c.has_value());
    ASSERT_TRUE(gen2_o.has_value());
    EXPECT_EQ(gen2_c->bytes, gen2_o->bytes);
    ASSERT_EQ(runs2_c.size(), 1u);
    ASSERT_EQ(runs2_o.size(), 1u);
    EXPECT_EQ(runs2_c[0].checksum, runs2_o[0].checksum);

    /// The core assertion: no whole-object get of the prior run key — every read carried a Range or a
    /// stream (the resident-memory proof at the seam).
    EXPECT_EQ(backend.wholeGetCount(gen1_run_key), 0u);
    /// The cursor opened the prior run's segment via the streaming reader (head + tail get + getStream).
    EXPECT_GE(backend.getStreamCount(gen1_run_key), 1u);
    /// Every ranged-get window on the prior run stays within one block + the footer allowance. This
    /// bound is strict here because the prior run's footer fits inside the fixed tail probe (only very
    /// large runs — ~13k blocks — spill the footer past the probe and add one exact-footer get; a note
    /// for that regime lives in the streaming reader's open comment).
    EXPECT_LE(backend.maxRangedGetLen(gen1_run_key),
              static_cast<uint64_t>(kLegacyHardCapBlockSize) + 64u * 1024u);
    /// Streaming open touches the prior run's tail probe (and at most one exact-footer get); it is never
    /// re-materialized whole.
    EXPECT_LE(backend.getCount(gen1_run_key), 2u);
}

/// The preview consumer `zeroInDegree` streams a multi-block run instead of materializing it whole: the
/// backend sees only block-bounded ranged/stream requests for the run key (never a whole-object get), and
/// the candidate set equals the pre-change (borrowed-mode) result. Byte-parity against an InMemory oracle
/// is the load-bearing canary — the scan logic is unchanged; only the byte source moved to the stream.
TEST(CASBlobInDegree, ZeroInDegreeStreamsBlockBounded)
{
    using DB::Cas::tests::CountingBackend;
    CountingBackend backend;
    InMemoryBackend oracle;
    Layout layout{"pool"};

    /// Gen 1 from empty prior: ~20000 active edges spill the SourceEdge run across several 256KB blocks.
    std::vector<BlobDelta> gen1;
    gen1.reserve(20000);
    for (uint64_t i = 0; i < 20000; ++i)
        gen1.push_back({bh(i), s(1), false});

    std::vector<RunRef> runs1_c;
    std::vector<RunRef> runs1_o;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, 0, 0, gen1, runs1_c);
    foldDeltasIntoGeneration(oracle, layout, /*prior_runs*/{}, 1, 0, 0, gen1, runs1_o);

    /// Gen 2 removes every edge on two of the blobs => two zero-transition markers in the gen-2 run,
    /// which is itself multi-block (the surviving-edge rows still span blocks).
    std::vector<BlobDelta> gen2{{bh(0), s(1), true}, {bh(19999), s(1), true}};
    std::vector<RunRef> runs2_c;
    std::vector<RunRef> runs2_o;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1_c, 2, 0, 0, gen2, runs2_c);
    foldDeltasIntoGeneration(oracle, layout, /*prior_runs*/runs1_o, 2, 0, 0, gen2, runs2_o);

    const String gen2_run_key = layout.blobTargetRunKey(2, 0, 0, 0);
    const auto gen2_run = backend.get(gen2_run_key);
    ASSERT_TRUE(gen2_run.has_value());
    /// Sanity: the run genuinely spans several blocks (else the block-bounded assertions are vacuous).
    ASSERT_GT(gen2_run->bytes.size(), static_cast<size_t>(kLegacyBlockSize) * 3);

    backend.resetCounts();
    const auto zero_c = zeroInDegree(backend, runs2_c);
    const auto zero_o = zeroInDegree(oracle, runs2_o);

    /// Equivalence with the borrowed-mode (InMemory oracle) result: same candidates, in the same order.
    ASSERT_EQ(zero_c.size(), zero_o.size());
    ASSERT_EQ(zero_c.size(), 2u);
    for (size_t i = 0; i < zero_c.size(); ++i)
        EXPECT_EQ(zero_c[i].ref, zero_o[i].ref);

    /// The core assertion: no whole-object get of the run key — every read carried a Range or a stream.
    EXPECT_EQ(backend.wholeGetCount(gen2_run_key), 0u);
    /// The scan opened the run via the streaming reader (head + tail get + getStream).
    EXPECT_GE(backend.getStreamCount(gen2_run_key), 1u);
    /// Every ranged-get window stays within one block + the footer allowance (the seam memory bound).
    EXPECT_LE(backend.maxRangedGetLen(gen2_run_key),
              static_cast<uint64_t>(kLegacyHardCapBlockSize) + 64u * 1024u);
    /// Streaming open touches the tail probe (and at most one exact-footer get); never re-materialized whole.
    EXPECT_LE(backend.getCount(gen2_run_key), 2u);
}

/// ==== kCondemned row codec + typed source-edge open (retired-in-snapshot T2, spec §2.1) ====

TEST(CASCondemnedRow, RoundTripAllTokenTypes)
{
    for (auto type : {DB::Cas::TokenType::ETag, DB::Cas::TokenType::Generation, DB::Cas::TokenType::Emulated})
    {
        DB::Cas::CondemnedRow row;
        row.delete_pending = (type == DB::Cas::TokenType::Generation);
        row.marker_confirmed = (type == DB::Cas::TokenType::Emulated);
        row.token = DB::Cas::Token{.value = "etag-abc-123", .type = type};
        row.size = 4096;
        row.condemn_round = 7;
        const auto bytes = DB::Cas::encodeCondemnedRow(row);
        ASSERT_EQ(bytes[0], DB::Cas::kCondemned);
        EXPECT_EQ(DB::Cas::decodeCondemnedRow(bytes), row);
    }
}

TEST(CASCondemnedRow, UnknownFlagBitsFailClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::Token{.value = "t", .type = DB::Cas::TokenType::ETag};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes[1] = 4;   // flags byte: only bits 0 (delete_pending) and 1 (marker_confirmed) are defined
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASCondemnedRow, UnknownTokenTypeFailsClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::Token{.value = "t", .type = DB::Cas::TokenType::ETag};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes[2] = 99;   // token_type byte (offset: [0]=0x02 [1]=flags [2]=token_type)
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASCondemnedRow, TruncatedPayloadFailsClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::Token{.value = "0123456789", .type = DB::Cas::TokenType::ETag};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes.resize(bytes.size() - 3);   // token bytes shorter than declared token_len
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASSourceEdgeRun, SourceEdgeIdZeroIsReserved)
{
    /// The zero source_id is the sentinel namespace; producers fail closed on a zero hash
    /// (probability 2^-128 — the check documents the reservation).
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            DB::Cas::assertValidSourceEdgeId(UInt128{0});
        },
        "source_id 0 is the reserved sentinel key");
    EXPECT_NO_THROW(DB::Cas::assertValidSourceEdgeId(UInt128{1}));
}

/// ==== schema 3 key codec (Phase 3 T3, mixed-algo pools) ====

TEST(CASSourceEdgeKeySchema3, MixedWidthKeysOrderAlgoFirst)
{
    const BlobDigest d16 = BlobDigest::fromU128((UInt128(0xFFFFFFFFFFFFFFFFULL) << 64) | 0xFFULL);
    BlobDigest d32{};                                    /// sha256 digest starting 0x00,0x01 — small bytes
    d32.bytes[1] = 0x01;
    const BlobRef ch{BlobHashAlgo::CityHash128, d16};    /// algo=1, digest all-FF prefix
    const BlobRef sh{BlobHashAlgo::Sha256, d32};         /// algo=3, tiny digest
    const String k_ch = SourceEdgeKeyCodec::key(ch, UInt128(7));   /// 33 bytes
    const String k_sh = SourceEdgeKeyCodec::key(sh, UInt128(7));   /// 49 bytes
    EXPECT_EQ(k_ch.size(), 33u);
    EXPECT_EQ(k_sh.size(), 49u);
    /// algo byte decides BEFORE any digest byte can: ch128(1) < sha256(3) even though the ch128
    /// digest bytes are all 0xFF and the sha256 digest bytes are almost all zero.
    EXPECT_LT(k_ch, k_sh);
    /// sentinel-first inside one blob group:
    EXPECT_LT(SourceEdgeKeyCodec::key(ch, UInt128(0)), k_ch);
}

TEST(CASSourceEdgeKeySchema3, ParseFailsClosed)
{
    BlobRef r; UInt128 sid;
    String k = SourceEdgeKeyCodec::key(BlobRef{BlobHashAlgo::XXH3_128, BlobDigest::fromU128(UInt128(5))}, UInt128(9));
    SourceEdgeKeyCodec::parse(k, r, sid);
    EXPECT_EQ(r.algo, BlobHashAlgo::XXH3_128);
    EXPECT_EQ(r.digest.toU128(), UInt128(5));
    EXPECT_EQ(sid, UInt128(9));
    k[0] = static_cast<char>(99);                        /// unknown algo byte
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]{ SourceEdgeKeyCodec::parse(k, r, sid); });
    k[0] = static_cast<char>(1);                         /// known algo, wrong length (33 expected, this is 33 — truncate)
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]{ SourceEdgeKeyCodec::parse(std::string_view(k).substr(0, 20), r, sid); });
}

TEST(CASBlobInDegree, TwoAlgoFoldSettlesBothInOneShardRun)
{
    /// Step 3 (Phase 3 T3): extend the fold with deltas for ch128:X and sha256:Y in ONE shard run —
    /// both settle (edges present, condemn on removal works per ref), mixed rows in one run, no
    /// algo loop.
    InMemoryBackend backend;
    Layout layout{"pool"};

    const BlobRef ch_x{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(11))};
    BlobDigest sha_y{};
    sha_y.bytes[0] = 0xAB;
    const BlobRef sha_y_ref{BlobHashAlgo::Sha256, sha_y};

    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, 1, /*attempt*/0, 0,
        {{ch_x, s(1), false}, {sha_y_ref, s(1), false}}, runs1);
    ASSERT_FALSE(runs1.empty());

    EXPECT_EQ(DB::Cas::tests::inDegreeInRuns(backend, runs1, ch_x), 1);
    EXPECT_EQ(DB::Cas::tests::inDegreeInRuns(backend, runs1, sha_y_ref), 1);
    EXPECT_TRUE(zeroInDegree(backend, runs1).empty());

    /// Remove both edges in gen 2: each transitions to zero independently, condemned per its own ref.
    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, 2, /*attempt*/0, 0,
        {{ch_x, s(1), true}, {sha_y_ref, s(1), true}}, runs2,
        /*current_round*/0, /*condemn_round*/1, headPresent("t", 1), /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    ASSERT_EQ(rmr.still_retired.size(), 2u);
    std::vector<BlobRef> condemned_refs{rmr.still_retired[0].ref, rmr.still_retired[1].ref};
    EXPECT_NE(std::find(condemned_refs.begin(), condemned_refs.end(), ch_x), condemned_refs.end());
    EXPECT_NE(std::find(condemned_refs.begin(), condemned_refs.end(), sha_y_ref), condemned_refs.end());
    EXPECT_EQ(DB::Cas::tests::inDegreeInRuns(backend, runs2, ch_x), 0);
    EXPECT_EQ(DB::Cas::tests::inDegreeInRuns(backend, runs2, sha_y_ref), 0);
}

/// [UNMATCHED-MINUS-ONE] pin. In-degree is a SET of source edges applied last-wins per
/// (ref, ManifestId, path) key -- NOT a counter. A removal delta whose matching activation was
/// never folded (reachable today via a false-404 at the activation fold plus a dead-build skip)
/// must therefore be a per-key NO-OP: it marks an already-absent edge absent and cannot strip a
/// sibling manifest's edge for the SAME blob. The whole "that interleaving is harmless" argument in
/// the publish-confirm design rests on this; if the model ever regresses to counter arithmetic this
/// test goes red and premature deletion becomes reachable again.
TEST(CASBlobInDegree, UnmatchedRemovalIsAPerKeyNoOpAndSparesSiblingEdges)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Generation 1: blob b1 is referenced by TWO distinct sources (two manifests).
    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, /*new_generation*/1, /*attempt*/0, /*shard*/0,
        {{bh(1), s(1), false}, {bh(1), s(2), false}}, runs1);

    /// Generation 2: fold a removal for a THIRD source that never had an activation folded.
    std::vector<RunRef> runs2;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, /*new_generation*/2, /*attempt*/0, /*shard*/0,
        {{bh(1), s(99), true}}, runs2);

    /// Both original edges survive: the unmatched removal touched only its own (absent) key.
    const DecodedRun out = decodeRun(backend, runs2[0]);
    ASSERT_EQ(out.edges.size(), 2u) << "an unmatched removal must not strip sibling edges";
    /// And the blob is NOT a deletion candidate.
    const auto zero = zeroInDegree(backend, runs2);
    EXPECT_TRUE(zero.empty()) << "b1 still has two live source edges";
}

/// The silence in `UnmatchedRemovalIsAPerKeyNoOpAndSparesSiblingEdges` above is exactly what let a whole
/// class of GC defects survive months of soak runs undetected — the fold's per-key no-op left no trace.
/// This test pins the COUNTING surface added on top: `RetiredMergeResult::unmatched_removes` /
/// `unmatched_remove_example` must report the unmatched remove precisely (one hit, naming the right blob
/// and source id), while the byte-level no-op behaviour (asserted above) is unchanged.
TEST(CASBlobInDegree, UnmatchedRemovalIsCountedWithAnExample)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    /// Generation 1: blob b1 is referenced by TWO distinct sources (two manifests), same fixture as the
    /// no-op test above.
    std::vector<RunRef> runs1;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, /*new_generation*/1, /*attempt*/0, /*shard*/0,
        {{bh(1), s(1), false}, {bh(1), s(2), false}}, runs1);

    /// Generation 2: fold a removal for a THIRD source that never had an activation folded.
    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/runs1, /*new_generation*/2, /*attempt*/0, /*shard*/0,
        {{bh(1), s(99), true}}, runs2,
        /*current_round*/0, /*condemn_round*/0, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{}, &rmr);

    /// The run is byte-identical to the no-op test's outcome for the blob's OTHER edges: both survive.
    const DecodedRun out = decodeRun(backend, runs2[0]);
    ASSERT_EQ(out.edges.size(), 2u) << "the counting surface must not perturb the no-op fold outcome";
    EXPECT_EQ(out.edges[0].first, b(1));
    EXPECT_EQ(out.edges[1].first, b(1));
    std::vector<UInt128> surviving_sources{out.edges[0].second, out.edges[1].second};
    EXPECT_NE(std::find(surviving_sources.begin(), surviving_sources.end(), s(1)), surviving_sources.end());
    EXPECT_NE(std::find(surviving_sources.begin(), surviving_sources.end(), s(2)), surviving_sources.end());

    /// The counting surface reports exactly the one unmatched remove, naming the right blob and source id.
    EXPECT_EQ(rmr.unmatched_removes, 1u);
    ASSERT_TRUE(rmr.unmatched_remove_example.has_value());
    EXPECT_EQ(rmr.unmatched_remove_example->ref, bh(1));
    EXPECT_EQ(rmr.unmatched_remove_example->source_id, s(99));
}

namespace
{
/// N distinct condemned rows for blobs b(1)..b(n), same shape `condemnedRowFor` produces, varying
/// only the token so distinct rows are trivially distinguishable in a failure message.
std::vector<std::pair<UInt128, CondemnedRow>> condemnedCohort(uint64_t n, uint64_t condemn_round, bool delete_pending)
{
    std::vector<std::pair<UInt128, CondemnedRow>> rows;
    for (uint64_t i = 1; i <= n; ++i)
        rows.push_back({b(i), condemnedRowFor(condemn_round, "t" + std::to_string(i), delete_pending)});
    return rows;
}
}

/// The redelete cohort is capped at `GcRoundWorkBudget::max_redeletes` per call. Excess
/// entries stay in `still_retired`, still `delete_pending`, to be redeleted by a later round — the
/// durable pipeline never loses one to the cap.
TEST(CASThreeCursorMerge, RedeleteBudgetCapsCohortAndCarriesExcess)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    const RunRef gen1 = writeSourceEdgeRun(backend, layout, 1, 0, 0, condemnedCohort(10, 1, /*delete_pending*/true));

    GcRoundWorkBudget budget;
    budget.max_redeletes = 3;

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{gen1}, 2, 0, 0, {}, runs2,
        /*current_round*/9, /*condemn_round*/9, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{},
        &rmr, /*suppress_destructive*/false, /*out_applied_by_txn_ordinal*/nullptr,
        /*source_retirements*/{}, &budget);

    EXPECT_EQ(rmr.redelete.size(), 3u);
    EXPECT_EQ(budget.redeletes_used, 3u);
    EXPECT_TRUE(rmr.graduated.empty());
    EXPECT_TRUE(rmr.spared.empty());
    ASSERT_EQ(rmr.still_retired.size(), 7u);
    for (const RetiredEntry & e : rmr.still_retired)
        EXPECT_TRUE(e.delete_pending) << "carried entries stay delete_pending, unexecuted this round";
}

/// Mirror test for the graduation cap: entries past `max_graduations` carry unchanged (still
/// condemned, NOT yet delete_pending) rather than being force-graduated; the floor re-evaluates them
/// next round.
TEST(CASThreeCursorMerge, GraduationBudgetCapsCohortAndCarriesExcess)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    const RunRef gen1 = writeSourceEdgeRun(backend, layout, 1, 0, 0, condemnedCohort(10, /*condemn_round*/1, /*delete_pending*/false));

    GcRoundWorkBudget budget;
    budget.max_graduations = 3;

    std::vector<RunRef> runs2;
    RetiredMergeResult rmr;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{gen1}, 2, 0, 0, {}, runs2,
        /*current_round*/5, /*condemn_round*/6, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{},
        &rmr, /*suppress_destructive*/false, /*out_applied_by_txn_ordinal*/nullptr,
        /*source_retirements*/{}, &budget);

    EXPECT_EQ(rmr.graduated.size(), 3u);
    EXPECT_EQ(budget.graduations_used, 3u);
    ASSERT_EQ(rmr.still_retired.size(), 10u);
    size_t pending_count = 0;
    size_t carried_count = 0;
    for (const RetiredEntry & e : rmr.still_retired)
        e.delete_pending ? ++pending_count : ++carried_count;
    EXPECT_EQ(pending_count, 3u)  << "only the graduated 3 are republished delete_pending";
    EXPECT_EQ(carried_count, 7u) << "the rest carry unchanged, still eligible next round";
}

/// The mandatory convergence proof: a cohort well past the per-round cap fully drains over
/// ceil(N / cap) rounds, feeding each round's output run back as the next round's prior — the exact
/// shape a real GC round repeats every pass.
TEST(CASThreeCursorMerge, RedeleteBudgetDrainsCohortToFixpointOverRounds)
{
    InMemoryBackend backend;
    Layout layout{"pool"};
    std::vector<RunRef> priors{writeSourceEdgeRun(backend, layout, 1, 0, 0, condemnedCohort(10, 1, /*delete_pending*/true))};

    uint64_t total_redeleted = 0;
    uint64_t rounds = 0;
    while (rounds < 10)
    {
        GcRoundWorkBudget budget;
        budget.max_redeletes = 3;
        std::vector<RunRef> out_runs;
        RetiredMergeResult rmr;
        foldDeltasIntoGeneration(backend, layout, priors, 2 + rounds, 0, 0, {}, out_runs,
            /*current_round*/100, /*condemn_round*/100, /*head_blob*/{}, /*peek_head*/{}, /*confirm_condemned_marker*/{},
            &rmr, /*suppress_destructive*/false, /*out_applied_by_txn_ordinal*/nullptr,
            /*source_retirements*/{}, &budget);
        total_redeleted += rmr.redelete.size();
        ++rounds;
        if (rmr.still_retired.empty())
            break;
        ASSERT_FALSE(out_runs.empty());
        priors = out_runs;
    }
    EXPECT_EQ(total_redeleted, 10u) << "no entry lost to the cap across the whole drain";
    EXPECT_EQ(rounds, 4u) << "ceil(10 / 3) rounds to fully drain";
}
