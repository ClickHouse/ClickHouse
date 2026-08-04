#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefLedger.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>

#include <Common/Exception.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

/// `prepareRefChunk` is the pure half of `commitRefChunk` (Stage B directive
/// `{#extract-prepare-ref-chunk}`): everything the append lane DECIDES before this chunk can have any
/// durable effect. This TU is where that purity is exercised, and it is deliberately backend-free --
/// nothing below names a backend, a pool, a ledger instance or a clock, and nothing constructs one. The
/// mechanical guarantee is `static` on `prepareRefChunk` itself: with no `this` there is no member
/// backend, runtime, clock or lock reachable from inside it, so a future edit cannot quietly reach for
/// one and still compile here.
///
/// What that buys is exactly what shows up below: every case is a direct call, so INV-2's chain-link
/// grammar is swept as a cross product -- including its negatives -- instead of being probed through
/// I/O.
///
/// The value the extraction protects is pinned elsewhere on purpose: the equivalence fences in
/// `gtest_cas_ref_ckpt.cpp` assert that the durable key, the sealed bytes and the per-key request
/// counts a REAL append produces are unchanged. Those need a backend, so they live there and this TU
/// stays pure.

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

using namespace DB::Cas;

namespace
{

const RootNamespace kNs{"srv1/prep@cas@"};
const Layout kLayout{"p"};
/// `prepareRefChunk` takes a resolved catalog life (Stage B, Task 4-C), not a bare namespace; this TU
/// is deliberately backend-free (no catalog to resolve one from), so it threads the Stage-A sentinel
/// through EXPLICITLY as its own test input -- the same value production minted internally before
/// Task 4-C, so every golden byte/key assertion below is unchanged.
const NamespaceLifeId kLife = DB::Cas::tests::fixture::fixtureLife(kNs);

RefOp birthOp()
{
    RefOp op;
    op.kind = RefOpKind::NamespaceBirth;
    return op;
}

RefOp epochSealOp()
{
    RefOp op;
    op.kind = RefOpKind::EpochSeal;
    return op;
}

/// A minimal content op: the `AddPrecommit` shape (a pure add of a PRECOMMIT owner). A committed owner
/// is only ever reached by promoting a precommit, so this is the smallest legal content transition.
RefOp addPrecommitOp(const String & ref_name, const ManifestRef & manifest)
{
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref_name, manifest};
    return op;
}

ManifestRef mref(uint64_t seq)
{
    return ManifestRef{1, seq, 1};
}

/// One live namespace, born at `{1,1}`, as the state a later chunk prepares against.
RefTableState bornState()
{
    RefTableState state;
    applyRefLogTxn(state, RefLogTxn{kNs.string(), RefTxnId{1, 1}, {birthOp()}, std::nullopt});
    return state;
}

/// Asserts that preparation REFUSES with `CORRUPTED_DATA` -- the code every ref-log grammar violation
/// normalises to -- and that the message names the chain link, so a row cannot pass because some
/// unrelated validator happened to throw first.
template <class F>
void expectGrammarRefusal(F && body, const char * what)
{
    try
    {
        std::forward<F>(body)();
        FAIL() << "expected a grammar refusal: " << what;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA) << what;
        EXPECT_NE(e.message().find("prev_epoch_seal"), String::npos)
            << what << " -- refused, but not by the chain-link rule; message: " << e.message();
    }
}

/// `prepareRefChunk` CONSUMES its state, so this copies -- which also lets every caller below assert
/// afterwards that its own state was left alone.
CasRefLedger::PreparedRefChunk prepare(const RefTableState & state, const RefTxnId & id,
                                       const std::optional<RefTxnId> & chain_link,
                                       const std::vector<RefOp> & ops, uint64_t admitted_generation = 7)
{
    return CasRefLedger::prepareRefChunk(kLayout, kLife, state, id, chain_link, ops, admitted_generation);
}

}

/// The two things that actually become durable -- the key and the sealed body -- are both derivable
/// before any request, and both round-trip: the key parses back to the life and id it names, and the
/// bytes decode back to the very transaction that was prepared.
TEST(CASRefChunkPreparation, PreparedKeyAndSealedBytesAreCanonical)
{
    const RefTxnId id{1, 2};   /// the contiguous successor of the born state's `1-1`
    const auto prepared = prepare(bornState(), id, std::nullopt, {addPrecommitOp("r1", mref(3))});

    const auto parsed = kLayout.parseRefObjectKey(prepared.prepared_attempt.key);
    ASSERT_TRUE(parsed.has_value()) << "the prepared key must be one of OUR ref-object keys";
    EXPECT_EQ(parsed->life_id, kLife.incarnation);
    EXPECT_EQ(parsed->kind, RefObjectKind::Log);
    EXPECT_EQ(parsed->txn_id, id);
    EXPECT_EQ(prepared.prepared_attempt.key, kLayout.refLogKey(kLife, id));

    const RefLogTxn decoded = decodeRefLogTxn(
        openObject(FormatId::RefLog, prepared.prepared_attempt.bytes), kNs.string(), id);
    EXPECT_EQ(decoded, prepared.chunk_txn) << "the sealed bytes must decode back to the prepared transaction";
    EXPECT_EQ(decoded.ns, kNs.string());
    EXPECT_EQ(decoded.txn_id, id);
    ASSERT_EQ(decoded.ops.size(), 1u);
    EXPECT_EQ(decoded.ops.front().kind, RefOpKind::OwnerTransition);
}

/// The base id a later install re-presents is the greatest-applied of the state preparation STARTED
/// from -- not of the candidate it produced. Getting this backwards would let an install adopt a
/// candidate over a state that had moved on.
TEST(CASRefChunkPreparation, CandidateBaseIdIsGreatestApplied)
{
    const RefTableState state = bornState();
    const RefTxnId base = state.getGreatestApplied();
    ASSERT_EQ(base, (RefTxnId{1, 1})) << "precondition: the born state's greatest-applied is its birth";

    const auto prepared = prepare(state, RefTxnId{1, 2}, std::nullopt, {addPrecommitOp("r1", mref(3))});
    EXPECT_EQ(prepared.candidate_base_id, base) << "the base id describes the state prepared FROM";
    EXPECT_EQ(prepared.candidate.getGreatestApplied(), (RefTxnId{1, 2}))
        << "the candidate itself has this chunk applied";
    /// `prepare` handed over a COPY, so the caller's state cannot have been advanced -- the property the
    /// real caller relies on when it re-presents `candidate_base_id` at install time.
    EXPECT_EQ(state.getGreatestApplied(), base) << "preparation must not mutate the caller's state";
}

/// INV-2's chain-link grammar across the full cross product. Preparation runs the real validators, so
/// this sweeps both directions: where the link is required or forbidden, an ill-formed combination must
/// be REFUSED here -- before anything is durable -- rather than sealed into bytes and PUT. That
/// two-sided sweep is what the extraction buys: it needs no backend, so there is no reason not to cover
/// the negatives too.
///
/// The base state is built per row, because a transaction id is only meaningful as the contiguous
/// successor of some stream (INV-1): a row cannot just assert a grammar rule on an id the stream would
/// never reach.
///
/// Note which validator each row lands on, because the two halves of the rule are DISJOINT and live in
/// different steps of preparation: the required-iff half is `validateEpochSealGrammarContextual`, run by
/// the candidate apply; the forbidden-off-sequence-1 half is `validateEpochSealGrammarStructural`, run
/// by `encodeRefLogTxn` during the seal. Both are inside preparation, which is the point -- a chunk that
/// passes one and fails the other still fails before any durable effect.
TEST(CASRefChunkPreparation, ChainLinkRequiredExactlyOnSequenceOneOfNonGenesisEpoch)
{
    const std::vector<RefOp> ops{addPrecommitOp("r1", mref(3))};
    const RefTxnId epoch1_seal{1, 5};   /// the seal that closed epoch 1

    /// From a namespace born at `1-1` (so `life_epoch == 1`).
    /// Sequence > 1 of the genesis epoch: the link is FORBIDDEN.
    EXPECT_NO_THROW(prepare(bornState(), RefTxnId{1, 2}, std::nullopt, ops))
        << "seq >1 with no link is the ordinary case";
    expectGrammarRefusal([&] { prepare(bornState(), RefTxnId{1, 2}, epoch1_seal, ops); },
                         "a link at sequence >1 is forbidden and must be refused before any durable effect");

    /// Sequence 1 of an epoch ABOVE genesis: the link is REQUIRED.
    EXPECT_NO_THROW(prepare(bornState(), RefTxnId{2, 1}, epoch1_seal, ops))
        << "seq 1 of a higher epoch names the seal that closed the previous one";
    expectGrammarRefusal([&] { prepare(bornState(), RefTxnId{2, 1}, std::nullopt, ops); },
                         "seq 1 of a higher epoch without a link must be refused -- 'no seal' is a fact "
                         "about the stream, not a defaulted field");

    /// Genesis itself: sequence 1 of the birth epoch has nothing to name, so a link is FORBIDDEN.
    EXPECT_NO_THROW(prepare(RefTableState{}, RefTxnId{3, 1}, std::nullopt, {birthOp()}))
        << "a genesis birth at sequence 1 finds nothing to name";
    expectGrammarRefusal([&] { prepare(RefTableState{}, RefTxnId{3, 1}, epoch1_seal, {birthOp()}); },
                         "a link on the birth transaction itself must be refused");

    /// Whatever the grammar admitted, the sealed bytes carry exactly that link and nothing else.
    const auto linked = prepare(bornState(), RefTxnId{2, 1}, epoch1_seal, ops);
    ASSERT_TRUE(linked.chunk_txn.prev_epoch_seal.has_value());
    EXPECT_EQ(*linked.chunk_txn.prev_epoch_seal, epoch1_seal);
    const RefLogTxn decoded = decodeRefLogTxn(
        openObject(FormatId::RefLog, linked.prepared_attempt.bytes), kNs.string(), RefTxnId{2, 1});
    EXPECT_EQ(decoded.prev_epoch_seal, linked.chunk_txn.prev_epoch_seal)
        << "the link must survive into the bytes that would become durable";
}

/// The birth `_ckpt` contribution is PREPARED here and published by `commitRefChunk`, because
/// publishing it is a birth chunk's first durable effect. Preparation therefore owes two things: the
/// value only for a birth, and the one fact no later writer can recover -- `life_epoch`.
TEST(CASRefChunkPreparation, BirthContributionSetOnlyForNamespaceBirth)
{
    /// A birth chunk at epoch 3: the contribution exists and names THIS transaction's writer epoch.
    const RefTxnId birth_id{3, 1};
    const auto born = prepare(RefTableState{}, birth_id, std::nullopt, {birthOp()});
    ASSERT_TRUE(born.birth_contribution.has_value());
    ASSERT_TRUE(born.birth_contribution->life_epoch.has_value());
    EXPECT_EQ(*born.birth_contribution->life_epoch, birth_id.writer_epoch);
    EXPECT_FALSE(born.birth_contribution->checkpoint_snapshot_id.has_value())
        << "the birth contributes life_epoch and nothing else -- the publisher owns the checkpoint field";
    EXPECT_FALSE(born.birth_contribution->last_epoch_seal.has_value());

    /// An ordinary content chunk contributes nothing: a second `_ckpt` write here would be a request the
    /// append lane does not owe.
    const auto ordinary = prepare(bornState(), RefTxnId{1, 2}, std::nullopt, {addPrecommitOp("r1", mref(3))});
    EXPECT_FALSE(ordinary.birth_contribution.has_value());

    /// A birth op mixed into a larger chunk still counts -- the check is over the whole chunk.
    const auto mixed = prepare(RefTableState{}, RefTxnId{5, 1}, std::nullopt,
                               {birthOp(), addPrecommitOp("r1", mref(3))});
    ASSERT_TRUE(mixed.birth_contribution.has_value());
    EXPECT_EQ(*mixed.birth_contribution->life_epoch, 5u);
}

TEST(CASRefChunkPreparation, CommitContributionCarriesFrontierAndOnlyMatchingSeal)
{
    const RefTxnId ordinary_id{1, 2};
    const auto ordinary = prepare(bornState(), ordinary_id, std::nullopt,
                                  {addPrecommitOp("r1", mref(3))});
    EXPECT_EQ(ordinary.commit_contribution.committed_through, ordinary_id);
    EXPECT_FALSE(ordinary.commit_contribution.life_epoch.has_value());
    EXPECT_FALSE(ordinary.commit_contribution.checkpoint_snapshot_id.has_value());
    EXPECT_FALSE(ordinary.commit_contribution.last_epoch_seal.has_value());

    const RefTxnId seal_id{1, 2};
    const auto seal = prepare(bornState(), seal_id, std::nullopt, {epochSealOp()});
    EXPECT_EQ(seal.commit_contribution.committed_through, seal_id);
    EXPECT_EQ(seal.commit_contribution.last_epoch_seal, seal_id)
        << "an epoch seal and its committed frontier must be one checkpoint contribution";
    EXPECT_FALSE(seal.commit_contribution.life_epoch.has_value());
    EXPECT_FALSE(seal.commit_contribution.checkpoint_snapshot_id.has_value());
}

/// The attempt exists so that an `Unresolved` PUT -- an object that may be durable -- can be recorded by
/// a MOVE and nothing else. That only holds if every field is already populated before the request goes
/// out, so this asserts the whole struct is complete at the end of preparation.
TEST(CASRefChunkPreparation, PreparedAttemptIsCompleteBeforeAnyDurableEffect)
{
    const RefTxnId id{1, 2};
    const auto prepared = prepare(bornState(), id, std::nullopt, {addPrecommitOp("r1", mref(3))}, /*admitted_generation=*/42);

    EXPECT_EQ(prepared.prepared_attempt.txn_id, id);
    EXPECT_FALSE(prepared.prepared_attempt.key.empty());
    EXPECT_FALSE(prepared.prepared_attempt.bytes.empty());
    EXPECT_EQ(prepared.prepared_attempt.admitted_fence_generation, 42u)
        << "the attempt carries the generation it was ADMITTED under, not a current reading";

    /// Nothing left to build: the key and body the request will read are already the canonical ones, so
    /// the arming block's only remaining work really is the move it declares itself to be.
    EXPECT_EQ(prepared.prepared_attempt.key, kLayout.refLogKey(kLife, id));
    EXPECT_EQ(prepared.prepared_attempt.bytes,
              sealObject(FormatId::RefLog, encodeRefLogTxn(prepared.chunk_txn)));
}
