#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcShardPlan.h>

using namespace DB::Cas;

namespace
{
BlobRef bh(uint64_t n) { return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(DB::UInt128(n))}; }
DB::UInt128 s(uint64_t n) { return DB::UInt128(n); }
}

/// The ledger is pure round-local bookkeeping; test it directly rather than trying to fabricate a
/// lost bucket inside a real fold. The fold-side wiring is covered by the gate: every existing GC
/// test now runs with the ledger armed and would throw if a delta went missing.
TEST(CASTxnApplyLedger, HealthyRoundReportsNothingUnapplied)
{
    TxnApplyLedger ledger;
    const uint32_t a = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    const uint32_t b = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 2});
    ledger.markProduced(a);
    ledger.markCommitted(a);
    ledger.markApplied(a);
    ledger.markCommitted(b);          /// committed but produced no blob deltas — legitimate
    EXPECT_TRUE(ledger.unapplied().empty());
}

TEST(CASTxnApplyLedger, CommittedAndProducedButNeverAppliedIsReported)
{
    TxnApplyLedger ledger;
    const uint32_t a = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    ledger.markProduced(a);
    ledger.markCommitted(a);
    ASSERT_EQ(ledger.unapplied().size(), 1u);
    EXPECT_EQ(ledger.unapplied().front(), a);
}

TEST(CASTxnApplyLedger, ClampedTransactionIsNotReported)
{
    /// A clamped log emits deltas into the per-log staging buffer that is then DISCARDED; it is never
    /// committed, so it must not be reported unapplied.
    TxnApplyLedger ledger;
    const uint32_t a = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    ledger.markProduced(a);
    EXPECT_TRUE(ledger.unapplied().empty());
}

/// The reducers mark `applied` by indexing the raw vector with `BlobDelta::txn_ordinal`, so the
/// ledger's own vectors must stay index-parallel with the ordinals it hands out. Pin that: the
/// ordinal is the position, and every parallel vector grows with it.
TEST(CASTxnApplyLedger, OrdinalsIndexTheParallelVectors)
{
    TxnApplyLedger ledger;
    EXPECT_EQ(ledger.open(RootNamespace{"a"}, RefTxnId{1, 7}), 0u);
    EXPECT_EQ(ledger.open(RootNamespace{"b"}, RefTxnId{2, 3}), 1u);
    ASSERT_EQ(ledger.applied.size(), 2u);
    ASSERT_EQ(ledger.produced.size(), 2u);
    ASSERT_EQ(ledger.committed.size(), 2u);
    ASSERT_EQ(ledger.namespaces.size(), 2u);
    EXPECT_EQ(ledger.namespaces[1], "b");
    EXPECT_EQ(ledger.txns[1], (RefTxnId{2, 3}));
}

/// PROBE B2's reach, pinned as a property rather than left to prose: a delta consumed by a reducer
/// clears its transaction, and only the transaction whose ordinal was never written stays reported.
/// This is the exact shape a delta lost in gc-shard routing produces.
TEST(CASTxnApplyLedger, OnlyTheTransactionWhoseDeltasVanishedIsReported)
{
    TxnApplyLedger ledger;
    const uint32_t routed = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    const uint32_t lost = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 2});
    for (const uint32_t o : {routed, lost})
    {
        ledger.markProduced(o);
        ledger.markCommitted(o);
    }
    /// The reducer's own write: a raw byte at the delta's ordinal, exactly as
    /// `foldDeltasIntoGeneration` performs it.
    ledger.applied[routed] = 1;

    ASSERT_EQ(ledger.unapplied().size(), 1u);
    EXPECT_EQ(ledger.unapplied().front(), lost);
}

/// The reducer-side half of probe B2, proven POSITIVELY rather than by the absence of a throw. The
/// three tests above exercise the ledger's own arithmetic; this one exercises the write that
/// `foldDeltasIntoGeneration` performs inside its delta-consumption loop — the only new code on the
/// fold's hot path — and pins that a routed delta marks its ordinal while an ordinal no delta carries
/// stays unmarked. Without this the fold-side wiring would only ever be covered negatively (the gate
/// does not throw), which cannot distinguish "the probe is correct" from "the probe is inert".
TEST(CASTxnApplyLedger, ReducerMarksTheOrdinalOfEveryDeltaItConsumes)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    TxnApplyLedger ledger;
    const uint32_t routed = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    const uint32_t absent = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 2});

    /// Only `routed`'s transaction emitted deltas. `absent`'s ordinal is live in the ledger but no
    /// delta carries it — exactly the shape a delta lost before the reducer produces.
    std::vector<BlobDelta> deltas{
        {bh(1), s(1), /*remove*/false, routed},
        {bh(2), s(1), /*remove*/false, routed},
    };
    std::vector<RunRef> runs;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, /*new_generation*/1, /*attempt*/0,
                             /*shard*/0, deltas, runs,
                             /*current_round*/0, /*condemn_round*/0, /*head_blob*/{}, /*peek_head*/{},
                             /*confirm_condemned_marker*/{}, /*out_retired*/nullptr,
                             /*suppress_destructive*/false, &ledger.applied);

    EXPECT_EQ(ledger.applied[routed], 1) << "the reducer consumed this transaction's deltas but did "
                                            "not mark its ordinal — probe B2 is inert";
    EXPECT_EQ(ledger.applied[absent], 0) << "an ordinal no delta carries must never be marked";

    /// And the verdict follows from those bits: a committed+produced transaction whose deltas never
    /// arrived is the one reported.
    for (const uint32_t o : {routed, absent})
    {
        ledger.markProduced(o);
        ledger.markCommitted(o);
    }
    ASSERT_EQ(ledger.unapplied().size(), 1u);
    EXPECT_EQ(ledger.unapplied().front(), absent);
}

/// The reducer must mark a REMOVAL delta too. Removals are the direction that can legitimately
/// collapse to nothing inside the set merge (an unmatched `-1` changes no state and emits no row), so
/// a mark placed at run flush instead of at consumption would silently skip exactly this case and
/// report a healthy round as lossy.
TEST(CASTxnApplyLedger, ReducerMarksAnUnmatchedRemovalDelta)
{
    InMemoryBackend backend;
    Layout layout{"pool"};

    TxnApplyLedger ledger;
    const uint32_t removal = ledger.open(RootNamespace{"ns"}, RefTxnId{1, 1});
    ledger.markProduced(removal);
    ledger.markCommitted(removal);

    /// A `-1` for an edge no prior run ever activated: a per-key no-op by design.
    std::vector<BlobDelta> deltas{{bh(1), s(1), /*remove*/true, removal}};
    std::vector<RunRef> runs;
    RetiredMergeResult merged;
    foldDeltasIntoGeneration(backend, layout, /*prior_runs*/{}, /*new_generation*/1, /*attempt*/0,
                             /*shard*/0, deltas, runs,
                             /*current_round*/0, /*condemn_round*/0, /*head_blob*/{}, /*peek_head*/{},
                             /*confirm_condemned_marker*/{}, &merged,
                             /*suppress_destructive*/false, &ledger.applied);

    EXPECT_EQ(merged.unmatched_removes, 1u) << "the fixture must actually stage an unmatched removal";
    EXPECT_EQ(ledger.applied[removal], 1);
    EXPECT_TRUE(ledger.unapplied().empty())
        << "a legitimate no-op removal must not read as a lost transaction";
}
