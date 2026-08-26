#include <benchmark/benchmark.h>

#include <algorithm>
#include <vector>

#include <Common/PODArray.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowMap.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>

/// Pure measurement, no pass/fail assertions -- see the cas-gc-rebuild BACKLOG.md entries
/// "OPTIMIZATION OPPORTUNITY -- ref-ledger JSON encoding writes byte-by-byte" and the (now
/// RESOLVED) "admits() re-encodes the WHOLE ref table once per state-growing op" entry for the
/// investigation these benchmarks measure. Build with `-DENABLE_BENCHMARKS=ON` and run the
/// resulting `benchmark_cas_ref_protocol` binary directly; never wired into `ninja test`
/// or CI.
///
/// BM_Admits history (synthetic RefTableState, time/call, this binary):
///   Before incremental admits() (2026-07-19) -- full O(N) rebuild+encode per call:
///     N=100: 48.8 us    N=1,000: 476 us    N=10,000: 5,018 us    N=100,000: 55,976 us
///     Google Benchmark complexity fit: O(N log N), RMS 2%.
///   After incremental admits() (2026-07-20) -- O(1) via incremental body-byte counters on
///   RefTableState:
///     N=100: 1842 ns    N=1,000: 1875 ns    N=10,000: 1864 ns    N=100,000: 1919 ns
///     Google Benchmark complexity fit: O(1), RMS 1-2%.
///
/// BM_EncodeRefLogTxn history (this binary; acceptance gate for the CasJsonWriter migration):
///   Before CasJsonWriter, field-by-field WriteBuffer calls (baseline): 753 ns.
///   After CasJsonWriter bulk-append migration (2026-07-20): 333 ns -- this is the shipped code.
///   BM_MemcpyTxnBytes floor (same bytes, plain String appends of 16-byte fragments): 30.7 ns.
///   Ratio EncodeRefLogTxn / MemcpyTxnBytes = 333 / 30.7 ~= 10.8x -- above the 3x acceptance gate.
///   A `keyLiteral` "rung-1" contingency variant (merging separator+key text into one literal
///   append for the fixed unprefixed keys in writeOp/writeCommittedRow) was also measured: 325 ns
///   ~= 10.8x -- a negligible ~2.5% move, not worth a third key-rendering path. It was NOT shipped;
///   writeOp/writeCommittedRow keep the single `writeKey` path for clarity. Per the contingency
///   ladder, rung 2 was NOT attempted either (it trades readability and needs a human decision);
///   reported as DONE_WITH_CONCERNS. CasEncodingPins.* stayed byte-identical (green) throughout.
///
/// Phase B baselines, 2026-07-21, pre-encapsulation (this binary; `--benchmark_repetitions=3
/// --benchmark_report_aggregates_only=true`; medians reported). Recorded ahead of the
/// `RefTableState` encapsulation refactor so later phases can re-run this exact suite unchanged and
/// diff against these numbers.
///   BM_Admits (promote op; stays O(1) via the incremental budget counters, untouched by this round):
///     N=100: 963 ns    N=1,000: 979 ns    N=10,000: 988 ns    N=100,000: 1,029 ns
///     Complexity fit: O(1), RMS 2%.
///   BM_AdmitsAddPrecommit (add op -- THE production hotspot shape: `manifestAlreadyOwned`'s linear
///   value scan AT THIS BASELINE; O(1) via the owned-manifest index since E2 -- see the Final block
///   below):
///     N=100: 995 ns    N=1,000: 4,266 ns    N=10,000: 38,771 ns    N=100,000: 400,222 ns
///     Complexity fit: O(N), ~4.0 ns/row, RMS 2%.
///   BM_ApplyRefLogTxn (scratch copy + validate + apply + install of one promote):
///     N=100: 724 ns    N=1,000: 738 ns    N=10,000: 784 ns    N=100,000: 788 ns
///     Complexity fit: O(1), RMS 4%.
///   BM_ReplayHistory (fold/recovery profile: snapshot of size N, 256 tail txns, 2 ops each):
///     N=100: 6.15 ms    N=1,000: 46.1 ms    N=10,000: 454.0 ms    N=100,000: 4.93 s
///     Complexity fit: O(N), ~48,859 ns/row, RMS 3%.
///   BM_ScratchCopy (one full RefTableState copy off a materialized state -- the isolation floor):
///     N=100: 45.7 ns    N=1,000: 46.0 ns    N=10,000: 46.7 ns    N=100,000: 46.8 ns
///     Complexity fit: O(1), RMS 1%.
///   BM_SnapshotEncode (encodeRefTableSnapshot(snapshotOf(state))):
///     N=100: 14,955 ns    N=1,000: 150,061 ns    N=10,000: 1,508,586 ns    N=100,000: 15,885,841 ns
///     Complexity fit: O(N), ~159 ns/row, RMS 1%.
///   BM_MergedIteration (full base + 10%-overlay merged iteration, post-copy pre-materialize shape):
///     N=100: 759 ns    N=1,000: 7,719 ns    N=10,000: 81,073 ns    N=100,000: 864,552 ns
///     Complexity fit: O(N), ~8.6 ns/row, RMS 4%.
///   BM_Materialize (RefCowMap::materialize after one overlay insert on an N-row base):
///     N=100: 12,069 ns    N=1,000: 126,687 ns    N=10,000: 1,296,326 ns    N=100,000: 18,145,559 ns
///     Complexity fit: O(N log N), RMS 2%.
///
/// Final, 2026-07-21, shipped tree (post E1+E2+E3; E4 tried and REVERTED -- full per-phase tables in
/// `bench_t5_e3.log`):
///   BM_AdmitsAddPrecommit: ~692-714 ns FLAT across N=100..100,000 -- O(1), RMS 1%
///     (the owned-manifest index replaced the linear scan; ~571x at N=100k).
///   BM_ReplayHistory: 1,725.58 ns/row (was 48,859) -- in-place `TrustedReplay` apply, -96.5%.
///   BM_ApplyRefLogTxn: ~778-822 ns O(1). BM_Admits (promote): ~996-1,056 ns O(1).
///   BM_ScratchCopy: ~58 ns O(1) (+~11 ns vs baseline: one more shared_ptr copy for the index).
///   BM_SnapshotEncode / BM_MergedIteration / BM_Materialize: unchanged from baseline (E4 reverted).
///
/// Implementation note for later phases: `makeSyntheticState` calls `RefCowMap::materialize()`
/// after `replay` (which never does -- it is the pure state-machine equation, and
/// `stateFromSnapshot` loads every row through `emplace`, which only ever touches the overlay).
/// Skipping that call makes every `RefTableState` copy in this suite (including `admits`'s and
/// `applyRefLogTxn`'s own internal scratch copies) an O(N) deep-copy of an un-materialized overlay
/// map instead of an O(1) shared-base copy -- this was caught during this round because it made
/// BM_Admits regress from the documented O(1) to visibly O(N log N), contradicting its own history
/// above. Production's RETAINED states are all materialized before reuse (the live table materializes
/// once per flush; post-consult the recovery-install site in CasRefLedger.cpp materializes the
/// replayed state before retaining it -- it previously did not, which is the recovery-latency cliff
/// BM_FlushInstall now measures against), so the fix was to materialize in the helper, not to accept
/// the contaminated numbers. (replay's own internal per-txn states are never materialized mid-fold;
/// BM_ReplayHistory models that path on purpose.)

using namespace DB::Cas;

namespace
{

/// A ref-ledger key shape as actually written on the wire: table_uuid + database + table + part_name.
constexpr std::string_view kSafeKeyLikeString
    = "eeeb74a2-606a-4ee9-840a-1aac7b5ac25b_ca_stress_default_part_20260719_0_89811_538";

RefLogTxn makeSamplePromoteTxn()
{
    RefLogTxn txn;
    txn.ns = "roots/ca_soak_ch1";
    txn.txn_id = RefTxnId{1, 12345};

    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "20260719_0_89811_538_89818", ManifestRef{1, 1, 999999}};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "20260719_0_89811_538_89818", ManifestRef{1, 1, 999999}};
    txn.ops.push_back(op);
    return txn;
}

/// A synthetic snapshot of `n` committed rows plus one pending precommit ready to promote.
/// Built as a RefTableSnapshot and materialized via the public `replay` entry point, so this
/// helper keeps compiling unchanged when RefTableState's fields become private (Phase A).
RefTableSnapshot makeSyntheticSnapshot(size_t n)
{
    RefTableSnapshot snapshot;
    snapshot.ns = "roots/bench";
    snapshot.snapshot_id = RefTxnId{1, 1};
    for (size_t i = 0; i < n; ++i)
    {
        RefCommittedRow row;
        row.ref_name = "part_" + std::to_string(i) + "_20260719_0_1000_1";
        row.manifest_ref = ManifestRef{1, 1, static_cast<uint32_t>(i + 1)};
        snapshot.committed.push_back(row);
    }
    std::sort(snapshot.committed.begin(), snapshot.committed.end(),
              [](const auto & a, const auto & b) { return a.ref_name < b.ref_name; });
    snapshot.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "new_part_x", ManifestRef{1, 1, 999999}});
    return snapshot;
}

/// A synthetic committed-ref table of `n` rows, plus one pending precommit ready to promote --
/// exactly the shape `admits()` previews on every state-growing ref op. Rebuilt through `replay`
/// (the public state-machine entry point) rather than by poking `RefTableState` fields directly,
/// so this helper survives Phase A's encapsulation of `RefTableState`.
///
/// `replay` (the pure state-machine equation) never materializes: `stateFromSnapshot` loads every
/// committed row through `RefCowMap::emplace`, which only ever touches the overlay. Left alone,
/// every subsequent `RefTableState` copy here (`admits`'s and `applyRefLogTxn`'s own internal
/// scratch copies, and every benchmark's own scratch copy below) would deep-copy an N-row overlay
/// map instead of sharing an immutable base pointer -- silently turning "the cost of the operation
/// under test" into "the cost of copying an un-materialized map" and swamping the O(1) `admits`
/// result the header history documents. The RETAINED long-lived states production keeps are all
/// materialized: the writer's live table materializes once per flush, and -- post-consult -- the
/// recovery-install site in `CasRefLedger.cpp` now calls `materializeCommitted()` on the replayed
/// state before retaining it (it previously did NOT, so the first flush copied an N-row overlay --
/// exactly the cliff this fix removed and the reason `BM_FlushInstall` below measures the fully
/// materialized flush cost). So this helper materializes too, matching what every real caller does
/// immediately after building or replaying a state it will keep. (Note that `replay`'s own INTERNAL
/// per-transaction states are never materialized mid-fold -- `BM_ReplayHistory` deliberately models
/// that, feeding `replay(snapshot, tail)` an un-materialized base on purpose.)
RefTableState makeSyntheticState(size_t n)
{
    RefTableState state = replay(makeSyntheticSnapshot(n), {});
    state.materializeCommitted();
    return state;
}

}

/// Floor comparison: writeJSONString's per-character escaping loop (WriteHelpers.h) on a string
/// that needs no escaping at all (a real ref-ledger key shape) vs a raw bulk write of the same
/// bytes. See BM_RawBulkWriteSafe below for the delta.
static void BM_WriteJSONStringSafe(benchmark::State & state)
{
    DB::FormatSettings settings;
    DB::PODArray<char> buf;
    for (auto _ : state)
    {
        buf.clear();
        DB::WriteBufferFromVector<DB::PODArray<char>> out(buf);
        DB::writeJSONString(kSafeKeyLikeString, out, settings);
        benchmark::DoNotOptimize(buf.data());
    }
}
BENCHMARK(BM_WriteJSONStringSafe);

static void BM_RawBulkWriteSafe(benchmark::State & state)
{
    DB::PODArray<char> buf;
    for (auto _ : state)
    {
        buf.clear();
        DB::WriteBufferFromVector<DB::PODArray<char>> out(buf);
        DB::writeChar('"', out);
        out.write(kSafeKeyLikeString.data(), kSafeKeyLikeString.size());
        DB::writeChar('"', out);
        benchmark::DoNotOptimize(buf.data());
    }
}
BENCHMARK(BM_RawBulkWriteSafe);

/// Absolute cost of encoding one ref-log transaction (a single promote op) with
/// `encodeRefLogTxn`'s migrated `CasJsonWriter` bulk-append implementation (see the history
/// comment at the top of this file and the BACKLOG resolution). `BM_MemcpyTxnBytes` right below
/// is the floor to diff this against.
static void BM_EncodeRefLogTxn(benchmark::State & state)
{
    const RefLogTxn txn = makeSamplePromoteTxn();
    for (auto _ : state)
        benchmark::DoNotOptimize(encodeRefLogTxn(txn));
}
BENCHMARK(BM_EncodeRefLogTxn);

/// The "near-memcpy" floor for BM_EncodeRefLogTxn: the SAME encoded bytes assembled from
/// precomputed 16-byte fragments by plain String appends -- approximating the writer's append
/// granularity with zero formatting/escaping work. Originally an acceptance gate for the
/// CasJsonWriter migration; measurement showed the <=3x-of-floor target is physically unreachable for a validating,
/// JSON-escaping encoder (BM_EncodeRefLogTxn lands at ~10.8x this floor even after the 2.26x
/// CasJsonWriter speedup -- see the BACKLOG resolution for the profiled breakdown). Kept as a
/// documented reference floor, not a pass/fail gate.
static void BM_MemcpyTxnBytes(benchmark::State & state)
{
    const RefLogTxn txn = makeSamplePromoteTxn();
    const String encoded = encodeRefLogTxn(txn);
    std::vector<std::string_view> fragments;
    constexpr size_t kFragment = 16;
    for (size_t off = 0; off < encoded.size(); off += kFragment)
        fragments.push_back(std::string_view(encoded).substr(off, kFragment));

    String buf;
    buf.reserve(encoded.size());
    for (auto _ : state)
    {
        buf.clear();
        for (const auto f : fragments)
            buf.append(f.data(), f.size());
        benchmark::DoNotOptimize(buf.data());
    }
}
BENCHMARK(BM_MemcpyTxnBytes);

/// admits() used to re-derive and re-encode the WHOLE committed-ref snapshot on every call
/// (CasRefProtocol.cpp), showing O(N log N) growth with table size; it now maintains
/// incremental body-byte counters on RefTableState instead, so this should show flat (O(1))
/// time/call across the range. ->Complexity() has Google Benchmark fit and print the
/// empirical big-O across the range.
static void BM_Admits(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableState table = makeSyntheticState(n);
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "new_part_x", ManifestRef{1, 1, 999999}};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "new_part_x", ManifestRef{1, 1, 999999}};

    for (auto _ : state)
        benchmark::DoNotOptimize(admits(table, op, 1ull << 40, 1ull << 40));

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_Admits)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// THE production hotspot shape: add-precommit runs `manifestAlreadyOwned` (a linear value scan
/// today). Expected O(N) before the experiments, O(1) after the winning combination. Unlike
/// BM_Admits (a promote, which never calls `manifestAlreadyOwned`), this previews a pure add --
/// the op every part publication starts with -- so it is the shape production traces show as
/// linear even after the incremental-budget fix landed for BM_Admits' promote shape.
static void BM_AdmitsAddPrecommit(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableState table = makeSyntheticState(n);
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "brand_new_part", ManifestRef{2, 1, 1}};

    for (auto _ : state)
        benchmark::DoNotOptimize(admits(table, op, 1ull << 40, 1ull << 40));

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_AdmitsAddPrecommit)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// One transaction end-to-end: scratch copy + validate + apply + install (a promote of the
/// staged precommit). The copy is part of the measured cost on purpose -- it is what E3 attacks.
static void BM_ApplyRefLogTxn(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableState table = makeSyntheticState(n);

    RefLogTxn txn;
    txn.ns = "roots/bench";
    txn.txn_id = RefTxnId{1, 2};
    RefOp promote;
    promote.kind = RefOpKind::OwnerTransition;
    promote.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "new_part_x", ManifestRef{1, 1, 999999}};
    promote.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "new_part_x", ManifestRef{1, 1, 999999}};
    txn.ops.push_back(promote);

    for (auto _ : state)
    {
        RefTableState scratch = table;
        applyRefLogTxn(scratch, txn);
        benchmark::DoNotOptimize(&scratch);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_ApplyRefLogTxn)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// End-to-end FLUSH-INSTALL cost: apply one state-growing transaction (add a fresh precommit, then
/// promote it -- touching BOTH the committed map AND the owned-manifest index) and then
/// `materializeCommitted()`, which folds BOTH COW overlays into fresh shared bases. THIS is the O(N)
/// critical section production holds `state_mutex` for, once per ref-log flush -- the number the
/// "writer path is flat" claim (drawn from `BM_ApplyRefLogTxn`, which stops before materialize) must be
/// weighed against. `BM_ApplyRefLogTxn` measures apply-without-install; the shipped-report
/// `BM_Materialize` measures only `RefCowMap`'s half; this measures the whole install including the
/// second (`owned_manifests`) container the index added, over the same N range.
static void BM_FlushInstall(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableState table = makeSyntheticState(n);   // materialized, as a live table is at a flush boundary

    /// add + promote of a fresh ref: the add inserts into `owned_manifests`, the promote grows
    /// `committed` -- so materialize below folds a nonempty overlay in BOTH containers. Manifest {4,1,1}
    /// and ref name are unique against the synthetic snapshot's {1,1,*} rows and "new_part_x" precommit.
    RefLogTxn txn;
    txn.ns = "roots/bench";
    txn.txn_id = RefTxnId{1, 2};
    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "flush_install_new_part", ManifestRef{4, 1, 1}};
    txn.ops.push_back(add);
    RefOp promote;
    promote.kind = RefOpKind::OwnerTransition;
    promote.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "flush_install_new_part", ManifestRef{4, 1, 1}};
    promote.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "flush_install_new_part", ManifestRef{4, 1, 1}};
    txn.ops.push_back(promote);

    for (auto _ : state)
    {
        RefTableState working = table;   // O(1): shared base
        applyRefLogTxn(working, txn);     // O(ops): bounded overlay
        working.materializeCommitted();   // O(N): the critical-section fold this benchmark exists to measure
        benchmark::DoNotOptimize(&working);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_FlushInstall)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// Same flush-install as `BM_FlushInstall`, but exercising the E5 uniquely-owned-base fast path that
/// production actually hits. `BM_FlushInstall` copies a shared fixture (`working = table`), so at
/// `materializeCommitted()` the base still has `use_count() == 2` and the fold must build a fresh
/// base -- O(N). Production's live table has NO outstanding scratch copy at the install point:
/// `CasRefLedger::flushRefBatch` EXPLICITLY releases its trial-validation copy (`working = RefTableState{}`)
/// before allocating the id and doing the post-PUT install, so at `materializeCommitted()` the live
/// base is uniquely owned and the fold happens in place -- O(overlay). This variant models that by
/// rebuilding a private,
/// materialized state each iteration (its base `use_count()` is 1), timing only the apply + in-place
/// materialize. The per-iteration rebuild AND the prior iteration's O(N) teardown are excluded from
/// the measurement by hoisting `working` out of the loop and rebuilding it via move-assignment under
/// Pause/ResumeTiming (the reassignment both destroys the previous grown state and installs a fresh
/// materialized one, all untimed). The residual per-iteration Pause/Resume overhead is a constant
/// floor, so the signal to read is FLATNESS across N (O(overlay)), not the absolute small-N number.
static void BM_FlushInstallUniqueOwner(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));

    RefLogTxn txn;
    txn.ns = "roots/bench";
    txn.txn_id = RefTxnId{1, 2};
    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "flush_install_new_part", ManifestRef{4, 1, 1}};
    txn.ops.push_back(add);
    RefOp promote;
    promote.kind = RefOpKind::OwnerTransition;
    promote.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "flush_install_new_part", ManifestRef{4, 1, 1}};
    promote.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "flush_install_new_part", ManifestRef{4, 1, 1}};
    txn.ops.push_back(promote);

    /// Hoisted out of the loop so the O(N) teardown of the previous iteration's grown state is folded
    /// into the untimed move-assignment below, not charged to the timed apply + materialize region.
    RefTableState working;
    for (auto _ : state)
    {
        state.PauseTiming();
        working = makeSyntheticState(n);   // private, materialized: base use_count() == 1
        state.ResumeTiming();

        applyRefLogTxn(working, txn);      // O(ops): bounded overlay
        working.materializeCommitted();    // O(overlay): uniquely-owned base folded IN PLACE (the E5 win)
        benchmark::DoNotOptimize(&working);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
/// Fixed iteration count: the E5 fast path makes the timed apply + in-place-materialize region tiny
/// and N-independent, so google-benchmark's default min-time targeting would demand millions of
/// iterations at every N -- each paying an untimed O(N) `makeSyntheticState` rebuild, which explodes
/// at large N. A fixed, modest count keeps every point cheap while still averaging enough samples to
/// read the flatness across N (the whole point of this variant).
BENCHMARK(BM_FlushInstallUniqueOwner)->RangeMultiplier(10)->Range(100, 100000)->Iterations(500)->Complexity();

/// The fold/recovery profile: K transactions replayed over a size-N snapshot. Each txn creates
/// and promotes one new ref (two ops), so each add pays today's `manifestAlreadyOwned` scan.
/// K fixed at 256; complexity fit is over N.
static void BM_ReplayHistory(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableSnapshot snapshot = makeSyntheticSnapshot(n);

    constexpr size_t kTailTxns = 256;
    std::vector<RefLogTxn> tail;
    tail.reserve(kTailTxns);
    for (size_t k = 0; k < kTailTxns; ++k)
    {
        RefLogTxn txn;
        txn.ns = "roots/bench";
        txn.txn_id = RefTxnId{1, 2 + k};

        /// Refs unique per k, and namespaced under writer_epoch 3 so they collide with nothing in
        /// the snapshot's own {1,1,i} committed series or its {1,1,999999} precommit.
        const String ref_name = "replay_part_" + std::to_string(k);
        const ManifestRef manifest_ref{3, 1, static_cast<uint32_t>(k + 1)};

        RefOp add;
        add.kind = RefOpKind::OwnerTransition;
        add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref_name, manifest_ref};
        txn.ops.push_back(add);

        RefOp promote;
        promote.kind = RefOpKind::OwnerTransition;
        promote.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, ref_name, manifest_ref};
        promote.new_binding = RefOwnerBinding{RefOwnerKind::Committed, ref_name, manifest_ref};
        txn.ops.push_back(promote);

        tail.push_back(std::move(txn));
    }

    for (auto _ : state)
        benchmark::DoNotOptimize(replay(snapshot, tail));

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_ReplayHistory)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// The isolation primitive on its own: one full state copy (COW committed + std::set precommits
/// + counters). Overlay is empty (state fresh from replay+materialize), so this is the floor.
static void BM_ScratchCopy(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    RefTableState table = makeSyntheticState(n);
    table.materializeCommitted();   /// makeSyntheticState already materializes; repeated here
                                     /// defensively (a no-op on an empty overlay) so this benchmark's
                                     /// floor claim does not silently depend on that helper's internals.

    for (auto _ : state)
    {
        RefTableState copy = table;
        benchmark::DoNotOptimize(&copy);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_ScratchCopy)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// Canonical snapshot encoding for size N (per-flush cost, expected O(N) -- the question is the
/// constant, which E4's contiguous scan attacks).
static void BM_SnapshotEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableState table = makeSyntheticState(n);

    for (auto _ : state)
        benchmark::DoNotOptimize(encodeRefTableSnapshot(snapshotOf(table, "roots/bench")));

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_SnapshotEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// Full merged iteration with a 10% overlay (post-copy, pre-materialize shape): an N-row
/// materialized base, then a fresh overlay of N/10 rows layered on top with `materialize()`
/// deliberately not called again -- so iteration must merge base and overlay in sorted order the
/// way the cold full-scan paths (snapshotOf, listRefs, dropNamespace) do against an in-flight batch.
/// Benchmarks `RefCowMap` directly (like `BM_Materialize` below) rather than through
/// `RefTableState::getCommitted()`: this isolates the merge-iteration primitive itself, and building
/// the overlay via `RefTableState`'s promote/precommit transactions would additionally measure the
/// state machine's own per-op bookkeeping, which is not what this benchmark is about.
static void BM_MergedIteration(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));

    RefCowMap map;
    for (size_t i = 0; i < n; ++i)
    {
        RefCommittedRow row;
        row.ref_name = "part_" + std::to_string(i) + "_20260719_0_1000_1";
        row.manifest_ref = ManifestRef{1, 1, static_cast<uint32_t>(i + 1)};
        map.emplace(row.ref_name, row);
    }
    map.materialize();

    const size_t overlay_n = std::max<size_t>(1, n / 10);
    for (size_t i = 0; i < overlay_n; ++i)
    {
        RefCommittedRow row;
        row.ref_name = "overlay_part_" + std::to_string(i) + "_20260719_0_1000_1";
        row.manifest_ref = ManifestRef{2, 1, static_cast<uint32_t>(i + 1)};
        map.insert_or_assign(row.ref_name, row);
    }

    for (auto _ : state)
    {
        size_t total = 0;
        for (const auto [ref_name, row] : map)
            total += row.ref_name.size();
        benchmark::DoNotOptimize(total);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_MergedIteration)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// RefCowMap::materialize after one overlay insert on an N-row base (per-flush install cost).
/// Benchmarks RefCowMap directly -- it is a public class.
static void BM_Materialize(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    RefCowMap base_map;
    for (size_t i = 0; i < n; ++i)
    {
        RefCommittedRow row;
        row.ref_name = "part_" + std::to_string(i) + "_20260719_0_1000_1";
        row.manifest_ref = ManifestRef{1, 1, static_cast<uint32_t>(i + 1)};
        base_map.emplace(row.ref_name, row);
    }
    base_map.materialize();

    for (auto _ : state)
    {
        RefCowMap copy = base_map;
        RefCommittedRow new_row;
        new_row.ref_name = "brand_new_part_20260719_0_1000_1";
        new_row.manifest_ref = ManifestRef{2, 1, 1};
        copy.insert_or_assign(new_row.ref_name, new_row);
        copy.materialize();
        benchmark::DoNotOptimize(&copy);
    }

    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_Materialize)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

BENCHMARK_MAIN();
