#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include <Common/PODArray.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowMap.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

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
/// NOTE (2026-08, wire-key-rename campaign): the "Phase B baselines" table immediately below measures
/// a DIFFERENT investigation (the `RefTableState` encapsulation refactor) and predates the five-format,
/// both-directions wire-key-cut design entirely. It is NOT the "before" side for that campaign's
/// measurement, and a later reader must not diff against it for that purpose. The actual before side
/// is the pre-cut worktree pinned at commit `65ec8688cdb`; the recorded patch that builds this file
/// there lives under `docs/superpowers/cas/bench-wire-keys-phase3/`. The table is kept exactly as
/// written because it is real history for the investigation it belongs to, not because it answers this
/// one -- see the "Wire-key-cut instrument" section further down for the five new formats this
/// campaign added.
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
///
/// Committed-row field widths (load-bearing for the `cas_ref_snap` wire-key-cut benchmarks and byte
/// oracle, which measure the RELATIVE cost of a key rename against the encoded VALUE bytes as the
/// denominator): `published_at_ms` is a real 13-digit epoch-ms rather than the default `0`, and
/// `manifest_ref`'s `writer_epoch`/`build_sequence` are multi-digit (a pool old enough to have
/// restarted its writer decades of times, and a build counter past its 89811th commit -- the same
/// order of magnitude as the real ref-ledger key at the top of this file, `kSafeKeyLikeString`, and
/// `makeSamplePromoteTxn`'s ref name). A minimal `0`/`1`/`1` shrinks the value-byte denominator a key
/// rename is measured against and inflates the rename's apparent percentage cost.
RefTableSnapshot makeSyntheticSnapshot(size_t n)
{
    RefTableSnapshot snapshot;
    snapshot.ns = "roots/bench";
    snapshot.snapshot_id = RefTxnId{1, 1};
    for (size_t i = 0; i < n; ++i)
    {
        RefCommittedRow row;
        row.ref_name = "part_" + std::to_string(i) + "_20260719_0_1000_1";
        row.manifest_ref = ManifestRef{42, 89811 + static_cast<uint64_t>(i), static_cast<uint32_t>(i + 1)};
        row.published_at_ms = 1752900000000ULL + i;
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

/// -------------------------------------------------------------------------------------------------
/// Wire-key-cut instrument (Task 7): encode AND decode for the five formats the campaign's wire-key
/// rename touched most (`cas_run`, `cas_ref_snap`, `cas_part_manifest`, `cas_fold_seal`,
/// `cas_ref_catalog`), plus a byte/cap oracle (below `reportFormatCaps`). This section only BUILDS the
/// instrument -- it does not take the before/after measurement itself, which is a later task run
/// against this same binary built on both sides of the cut. The "before" side is the pre-cut worktree
/// at `/home/mfilimonov/workspace/ClickHouse/cas-p2-before`, pinned at commit `65ec8688cdb`; the
/// recorded patch that adapts this file's one incompatible call site (`foldedClassification`/
/// `clampedClassification` below) for that build lives under
/// `docs/superpowers/cas/bench-wire-keys-phase3/`. Every other line in this section is byte-identical
/// on both sides -- confirmed against the before-side headers, which differ from these only in
/// comment text (the retired terse wire spellings) and in `RefCoverage::classification`'s type.
/// -------------------------------------------------------------------------------------------------

namespace
{

/// `cas_run` fixture: `n` distinct blobs in strictly ascending digest order (`SourceEdgeRunWriter`
/// requires non-decreasing `(ref, source_id)` keys, and a monotonically increasing digest alone
/// satisfies that regardless of `source_id`). Marker mix models one healthy in-degree run: the
/// overwhelming majority of tracked blobs simply carry a live edge this generation (98% `Edge`); a
/// blob losing its LAST edge (`Zero`) or actually condemned for deletion (`Condemned`, carrying the
/// full retired-incarnation token) is comparatively rare at any one round -- 1% each here, not 0 and
/// not half. `source_id` is a synthetic per-record counter rather than a real backend id: the codec's
/// cost is driven by the DIGEST's hex width, not the id's numeric value. The condemned token mirrors a
/// real S3 ETag's width (a quoted 32-hex value) and `size` a realistic single-blob byte count (64 KiB,
/// a typical compressed column chunk). Record count ranges 100 to 100,000 (`RangeMultiplier(10)`),
/// matching every `Complexity()` benchmark already in this file.
std::vector<SourceEdgeRecord> makeSourceEdgeRecords(size_t n)
{
    std::vector<SourceEdgeRecord> records;
    records.reserve(n);
    for (size_t i = 0; i < n; ++i)
    {
        const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(i + 1))};
        SourceEdgeRecord rec;
        rec.ref = ref;
        if (i % 100 == 0)
        {
            rec.source_id = UInt128(0);
            rec.marker = RunMarker::Condemned;
            rec.delete_pending = (i % 200 == 0);
            rec.token = PersistedEtag{"etag", "\"e1b2c3d4e5f6071829300a0b0c0d0e0f\""};
            rec.size = 64 * 1024;
            rec.condemn_round = 7;
        }
        else if (i % 100 == 50)
        {
            rec.source_id = UInt128(0);
            rec.marker = RunMarker::Zero;
        }
        else
        {
            rec.source_id = UInt128(i + 1);
            rec.marker = RunMarker::Edge;
        }
        records.push_back(rec);
    }
    return records;
}

/// Runs the real `SourceEdgeRunWriter` over `records`, exactly as `CASRecordStream`'s own
/// `encodeRun` helper does -- so decode below always consumes real encoder output, never a
/// hand-built string.
String encodeSourceEdgeRun(const std::vector<SourceEdgeRecord> & records)
{
    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    for (const auto & r : records)
        writer.append(r);
    writer.finish();
    /// `str()` returns a `std::string &`, so returning it plainly would copy-construct the whole
    /// encoded run on every call (no NRVO is available for a reference) -- `std::move` here moves it
    /// instead, matching the four other encoders, which all end with `std::move(out).take()` and copy
    /// nothing. `str()` finalizes `out` itself, so no separate `finalize()` call is needed first.
    return std::move(out.str());
}

/// The ONE call site whose TYPE differs across the wire-key-rename cut this benchmark spans: on this
/// (AFTER) side `RefCoverage::classification` is the closed `CoverageClass` enum; at the pre-cut
/// commit it is a raw `uint8_t` whose CLAMPED value is ALSO renumbered (4 there, 3 here -- see
/// `CasFoldSealFormat.h`'s own history comment on `CoverageClass`). A bare numeric literal at the call
/// site would therefore silently measure the WRONG row shape on the before-side build, so the Step-4
/// patch touches only this pair of one-line functions; every benchmark body in this file stays
/// byte-identical on both sides.
CoverageClass foldedClassification() { return CoverageClass::Folded; }
CoverageClass clampedClassification() { return CoverageClass::Clamped; }

/// Not the record axis under test (`n` below is `ref_lives` row count): fixed at a representative
/// multi-shard pool size. A single-shard fixture would fold `blob_target_runs`/`condemned_summary` to
/// one degenerate entry each, understating the per-shard fan-out a real multi-shard pool carries in
/// both sections.
constexpr uint64_t kFoldSealGcShards = 4;

/// `cas_fold_seal` fixture: `n` `ref_lives` rows keyed by ascending life id, split base/hold-bearing/
/// cleanup-evidence 90%/5%/5%. Per the spec's byte table, a hold-bearing row adds 33 bytes and a
/// cleanup-evidence row adds 16 bytes over a base row's 22-plus-class-word bytes; at this 90/5/5 mix
/// the recovered uplift over an all-base fixture is 0.05*33 + 0.05*16 = 2.45 bytes/row, about 8% over
/// a base row's own ~30 bytes (the full one-third the spec's deltas imply is the all-clamped extreme,
/// not this mix) -- still enough that omitting the two minority shapes entirely would misstate the
/// row-average cost in the wrong direction. The 90/5/5 split models a healthy pool: most namespaces
/// fold cleanly every round (base: `Folded`, no hold, no cleanup evidence); a minority sit behind a
/// transient barrier (hold-bearing: `Clamped`, `ManifestBodyMissing`); a minority are mid-teardown
/// (cleanup evidence: `Folded` plus a terminal `remove_namespace` fold). Neither minority shape is the
/// common case, but neither is negligible either -- both recur every round in a live pool.
/// `RefTxnId` epoch/sequence pairs and the hold's `retry_count`/`next_retry_round` are multi-digit
/// (a pool old enough to have restarted its writer dozens of times and folded past its 100,000th
/// ref-log transaction; a hold retried past its first round but nowhere near abandoned) rather than
/// the single-digit illustrative values the spec's byte table uses to name the three row SHAPES --
/// matching the shapes, not the spec table's example digits, is what keeps the value-byte denominator
/// realistic (see `makeSyntheticSnapshot`'s doc comment for why that denominator matters). Record
/// count ranges 100 to 100,000, matching every `Complexity()` benchmark in this file.
CasFoldSeal makeFoldSeal(size_t n)
{
    CasFoldSeal seal;
    seal.generation = 7;
    seal.parent_generation = 6;
    for (size_t i = 0; i < n; ++i)
    {
        RefLifeFoldState row;
        if (i % 20 == 0)
        {
            row.coverage = RefCoverage{
                .classification = clampedClassification(),
                .last_folded_ref_id = RefTxnId{42, 103482},
                .hold = RefHold{
                    .reason = HoldReason::ManifestBodyMissing,
                    .offending_position = RefTxnId{42, 103500},
                    .retry_count = 14,
                    .next_retry_round = 1042}};
        }
        else if (i % 20 == 1)
        {
            row.coverage = RefCoverage{.classification = foldedClassification(), .last_folded_ref_id = RefTxnId{42, 118203}};
            row.cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{42, 118190}};
        }
        else
        {
            row.coverage = RefCoverage{.classification = foldedClassification(), .last_folded_ref_id = RefTxnId{42, 100123}};
        }
        seal.ref_lives.emplace(UInt128(i + 1), std::move(row));
    }
    for (uint64_t shard = 0; shard < kFoldSealGcShards; ++shard)
    {
        seal.blob_target_runs.push_back(RunRef{
            .key = fmt::format("p/gc/gen/7/attempt/1/blob_target/{}/0", shard),
            .checksum = UInt128(0x1000 + shard), .shard = shard, .key_generation = 7});
        seal.condemned_summary[shard] = CondemnedSummary{
            .condemned_total = 1000 + shard, .pending_total = 10 + shard, .oldest_nonpending_condemn_round = 4};
    }
    return seal;
}

/// `cas_part_manifest` fixture: `n` entries in path order, 90% `Blob` (the column/mark/index files
/// that dominate a real MergeTree part) and every 10th `Inline` (small metadata files like
/// `count.txt`/`checksums.txt` that get embedded rather than stored as a separate blob). Blob sizes
/// cycle 4-64 KiB across 16 steps to resemble the spread of real column-chunk sizes rather than one
/// repeated constant; inline bytes are a fixed 48-byte payload, resembling a small metadata file.
/// `ref`/`root_namespace_id` are fixed -- they do not scale with entry count in a real manifest
/// either. `encodePartManifest` sorts entries itself, so input order need not be canonical. Record
/// count ranges 100 to 100,000, matching every `Complexity()` benchmark in this file (a real part
/// rarely reaches the top of that range; it stress-tests a pathologically wide/many-column part).
PartManifest makePartManifest(size_t n)
{
    PartManifest m;
    m.ref = ManifestRef{5, 15, 1};
    m.root_namespace_id = RootNamespace("00/aa@cas@");
    m.entries.reserve(n);
    for (size_t i = 0; i < n; ++i)
    {
        ManifestEntry e;
        if (i % 10 == 9)
        {
            e.path = fmt::format("{:06}_meta.txt", i);
            e.placement = EntryPlacement::Inline;
            e.inline_bytes = String(48, 'x');
        }
        else
        {
            e.path = fmt::format("{:06}_data.bin", i);
            e.placement = EntryPlacement::Blob;
            e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(i + 1))};
            e.blob_size = 4096 * (1 + (i % 16));
        }
        m.entries.push_back(std::move(e));
    }
    m.payload_digest = computePayloadDigest(m);
    return m;
}

/// `cas_ref_catalog` fixture: `n` entries in ascending namespace order (a 7-digit zero-padded ordinal
/// keeps ascending lexical order across the whole 100..100,000 range, well under `kMaxNamespaceBytes`).
/// The mix resembles one whole-pool catalog snapshot: most namespaces are simply `Live` (96%), with a
/// small steady trickle of admission (`Creating`, 2%) and teardown (`Removing`, 2%) in flight at any
/// moment -- neither churn state is the common case, but neither is negligible either. Record count
/// ranges 100 to 100,000, matching every `Complexity()` benchmark in this file.
RefCatalog makeRefCatalog(size_t n)
{
    RefCatalog catalog;
    catalog.entries.reserve(n);
    for (size_t i = 0; i < n; ++i)
    {
        CatalogEntry e;
        e.ns = RootNamespace(fmt::format("roots/ca_tbl_{:07}", i));
        e.incarnation = UInt128(i + 1);
        if (i % 50 == 0)
        {
            e.state = NsState::Creating;
            e.creator = CreatorFence{"srv-bench", 1, 1};
        }
        else if (i % 50 == 25)
        {
            e.state = NsState::Removing;
            e.removal_started_round = 42;
        }
        else
        {
            e.state = NsState::Live;
        }
        catalog.entries.push_back(std::move(e));
    }
    return catalog;
}

}

/// `cas_run` is streamed (`object_cap == 0`; see `CasRecordStreamFormat.h`) and never materialized
/// whole in production, but the benchmark still needs one complete encoded run to time and to decode:
/// `encodeSourceEdgeRun` drives the real `SourceEdgeRunWriter`/`SourceEdgeRunReader` pair over an
/// in-memory buffer, the same pair the streaming production path uses over its own `WriteBuffer`/
/// `ReadBuffer`.
static void BM_CasRunEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const std::vector<SourceEdgeRecord> records = makeSourceEdgeRecords(n);
    for (auto _ : state)
        benchmark::DoNotOptimize(encodeSourceEdgeRun(records));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRunEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasRunDecode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const String encoded = encodeSourceEdgeRun(makeSourceEdgeRecords(n));
    for (auto _ : state)
    {
        DB::ReadBufferFromMemory in(encoded.data(), encoded.size());
        SourceEdgeRunReader reader(in);
        SourceEdgeRecord rec;
        size_t count = 0;
        while (reader.next(rec))
            ++count;
        benchmark::DoNotOptimize(count);
    }
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRunDecode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

/// `BM_SnapshotEncode` above already exists (for the E4 contiguous-scan investigation) and has no
/// decode counterpart. This pair is the one the wire-key-cut measurement uses: same fixture, but named
/// and shaped to match the other four formats' encode/decode pairs in this section.
static void BM_CasRefSnapEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableSnapshot snapshot = makeSyntheticSnapshot(n);
    for (auto _ : state)
        benchmark::DoNotOptimize(encodeRefTableSnapshot(snapshot));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRefSnapEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasRefSnapDecode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefTableSnapshot snapshot = makeSyntheticSnapshot(n);
    const String encoded = encodeRefTableSnapshot(snapshot);
    for (auto _ : state)
        benchmark::DoNotOptimize(decodeRefTableSnapshot(encoded, snapshot.ns, snapshot.snapshot_id));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRefSnapDecode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasPartManifestEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const PartManifest m = makePartManifest(n);
    for (auto _ : state)
        benchmark::DoNotOptimize(encodePartManifest(m));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasPartManifestEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasPartManifestDecode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const PartManifest m = makePartManifest(n);
    const String encoded = encodePartManifest(m);
    for (auto _ : state)
        benchmark::DoNotOptimize(decodePartManifest(encoded));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasPartManifestDecode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasFoldSealEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const CasFoldSeal seal = makeFoldSeal(n);
    for (auto _ : state)
        benchmark::DoNotOptimize(encodeFoldSeal(seal));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasFoldSealEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasFoldSealDecode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const CasFoldSeal seal = makeFoldSeal(n);
    const String encoded = encodeFoldSeal(seal);
    for (auto _ : state)
        benchmark::DoNotOptimize(decodeFoldSeal(encoded));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasFoldSealDecode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasRefCatalogEncode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefCatalog catalog = makeRefCatalog(n);
    for (auto _ : state)
        benchmark::DoNotOptimize(encodeRefCatalog(catalog));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRefCatalogEncode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

static void BM_CasRefCatalogDecode(benchmark::State & state)
{
    const size_t n = static_cast<size_t>(state.range(0));
    const RefCatalog catalog = makeRefCatalog(n);
    const String encoded = encodeRefCatalog(catalog);
    for (auto _ : state)
        benchmark::DoNotOptimize(decodeRefCatalog(encoded));
    state.SetComplexityN(static_cast<int64_t>(n));
}
BENCHMARK(BM_CasRefCatalogDecode)->RangeMultiplier(10)->Range(100, 100000)->Complexity();

namespace
{

/// Binary search on record count with the real encode -> `sealObject` -> `openObject` pipeline as the
/// oracle. `openObject` (`CasTextFormat.cpp`) enforces the registry's `object_cap` on BOTH the raw and
/// the zstd-frame-header path, so this one pipeline works whether or not the format compresses; a
/// format's OWN pre-put gate (e.g. fold-seal's `checkFoldSealObjectBytes`) may throw earlier, at the
/// encode step itself. Either `LIMIT_EXCEEDED` or `CORRUPTED_DATA` at this boundary means "does not
/// fit" and steers the search; any other exception is a fixture bug, not a capacity signal, and is
/// left to propagate rather than being misread as "found the cap".
///
/// `known_fits_n`/`known_fits_bytes` seed the exponential search from a bytes-per-record estimate
/// measured at a small `n` -- NOT a hardcoded delta table -- purely to reduce how many large,
/// expensive encodes the search performs before bisecting. The estimate never becomes the answer: the
/// real encoder confirms every step of both the exponential growth and the final exact bisection.
template <typename Encode>
uint64_t maxRecordCountUnderCap(FormatId id, uint64_t known_fits_n, uint64_t known_fits_bytes, Encode encode)
{
    auto fits = [&](uint64_t n) -> bool
    {
        try
        {
            const String stored = sealObject(id, encode(n));
            benchmark::DoNotOptimize(openObject(id, stored));
            return true;
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == DB::ErrorCodes::CORRUPTED_DATA || e.code() == DB::ErrorCodes::LIMIT_EXCEEDED)
                return false;
            throw;
        }
    };

    /// The bisection below is correct only if `fits(lo) == true`. The caller's `known_fits_n` comes
    /// from an encode IT ran itself -- never through `openObject`, which is what actually enforces
    /// `object_cap` (the raw-size check, or the zstd frame's declared decompressed size) -- so this
    /// verifies the bound directly rather than trusting that claim. If `known_fits_n` itself is
    /// already over the cap (e.g. a future, much larger report size), halve downward until a verified
    /// fit is found; if even `n == 1` does not fit, that is a fixture/format bug, not a capacity
    /// signal, and is raised loudly rather than silently reported as a wrong maximum.
    uint64_t lo = known_fits_n;
    while (lo > 1 && !fits(lo))
        lo /= 2;
    if (!fits(lo))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR,
            "maxRecordCountUnderCap: format {} does not fit its object cap even at n=1", static_cast<uint16_t>(id));

    const FormatTraits & traits = traitsFor(id);
    const uint64_t per_record = std::max<uint64_t>(1, known_fits_bytes / std::max<uint64_t>(1, known_fits_n));
    uint64_t hi = std::max<uint64_t>(lo * 2, traits.object_cap / per_record);
    while (fits(hi))
    {
        lo = hi;
        hi *= 2;
    }
    while (hi - lo > 1)
    {
        const uint64_t mid = lo + (hi - lo) / 2;
        (fits(mid) ? lo : hi) = mid;
    }
    return lo;
}

/// One format's report line: decompressed bytes at `report_n`, stored bytes under the format's REAL
/// registered compression policy (or `n/a` for a policy that stores raw -- `Never`/`PinnedRaw` -- since
/// there is no separate compressed form to report, and a `0` there would read as a measurement rather
/// than "not applicable"), and the largest record count the real encoder admits under the format's
/// object cap.
template <typename Encode>
void reportSealedFormat(std::string_view name, FormatId id, uint64_t report_n, Encode encode)
{
    const FormatTraits & traits = traitsFor(id);
    const String decompressed = encode(report_n);
    const String stored = sealObject(id, decompressed);
    const bool stores_raw = traits.compression != CompressionPolicy::Always;
    const uint64_t max_n = maxRecordCountUnderCap(id, report_n, decompressed.size(), encode);

    fmt::print("{:<18} decompressed={:>10} bytes (n={})  stored={}  max_n_under_object_cap={}\n",
        name, decompressed.size(), report_n,
        stores_raw ? "n/a (stored raw, no compression)" : (std::to_string(stored.size()) + " bytes (zstd)"),
        max_n);
}

/// Step 3's byte and cap oracle: a small, main-less, flag-invoked harness (see `main` below) rather
/// than a benchmark or a gtest, so it never engages the timing loop and never needs a second `main` in
/// this binary. Reports, per format, at the stated `kReportN`: decompressed bytes, stored bytes under
/// the real compression policy (or `n/a`), and the maximum record count the real encoder admits under
/// the object cap (or `n/a` where none applies).
void reportFormatCaps()
{
    constexpr uint64_t kReportN = 1000;
    fmt::print("=== cas format byte/cap report (n={}) ===\n", kReportN);

    /// `cas_run` is `object_cap == 0` (streamed, `RunFile` family): never materialized whole in
    /// production, so there is no whole-object cap to search for and no compressed form to report.
    {
        const String encoded = encodeSourceEdgeRun(makeSourceEdgeRecords(kReportN));
        fmt::print("{:<18} decompressed={:>10} bytes (n={})  stored=n/a (PinnedRaw, never compressed)  "
                   "max_n_under_object_cap=n/a (object_cap=0: streamed one line at a time, never materialized whole)\n",
                   "cas_run", encoded.size(), kReportN);
    }

    reportSealedFormat("cas_ref_snap", FormatId::RefSnapshot, kReportN,
        [](uint64_t n) { return encodeRefTableSnapshot(makeSyntheticSnapshot(n)); });
    reportSealedFormat("cas_part_manifest", FormatId::PartManifest, kReportN,
        [](uint64_t n) { return encodePartManifest(makePartManifest(n)); });
    reportSealedFormat("cas_fold_seal", FormatId::FoldSeal, kReportN,
        [](uint64_t n) { return encodeFoldSeal(makeFoldSeal(n)); });
    reportSealedFormat("cas_ref_catalog", FormatId::RefCatalog, kReportN,
        [](uint64_t n) { return encodeRefCatalog(makeRefCatalog(n)); });
}

}

/// Hand-written in place of `BENCHMARK_MAIN()` so `--report_format_caps` can dispatch to Step 3's
/// oracle BEFORE `benchmark::Initialize` ever sees argv -- keeping the byte/cap report in this same
/// binary without a second `main` or a separate gtest target, and without the report's args tripping
/// `ReportUnrecognizedArguments`. Absent that flag, behavior is exactly `BENCHMARK_MAIN()`'s.
int main(int argc, char ** argv)
{
    for (int i = 1; i < argc; ++i)
    {
        if (std::string_view(argv[i]) == "--report_format_caps")
        {
            reportFormatCaps();
            return 0;
        }
    }

    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
