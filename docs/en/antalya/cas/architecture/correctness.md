---
description: 'What the TLA+ model corpus proves about CAS safety, the counterexamples that shaped the design, and how model-checking and soak/chaos testing complement each other.'
sidebar_label: 'Correctness'
sidebar_position: 12
slug: /antalya/cas/architecture/correctness
title: 'CAS Architecture — Correctness'
doc_type: 'reference'
---

# CAS architecture — correctness {#correctness}

`CAS` treats formal modelling as a pre-implementation gate, not after-the-fact documentation. No
task that changes safety-relevant behavior starts until the relevant `TLA+` model is green, and
"green" means every safety and liveness stage holds **and** every deliberately sabotaged variant
(`sab_*`) violates the specific rule it targets. A sabotage that fails to reproduce its named
counterexample is treated as seriously as a real violation — it means the model was not actually
covering the case it claimed to cover. This is why every safety rule below ships with the
counterexample that appears when you remove it.

The full model index (source `.tla` files and proof-run records) lives at
`docs/superpowers/models/`; this page is the reader-facing summary.

## Model → invariant → counterexample {#model-invariant-counterexample}

| Model (`docs/superpowers/models/`) | Invariant it proves | Counterexample it caught |
|---|---|---|
| `CaBlobPublishCore.tla` | Split `HEAD` from unconditional publication while preserving logical content, fresh incarnation identity, monotonic `publication_attempted`, fence safety, and readiness only after metadata reconciliation | Eleven sabotage configurations cover condemned adoption, stale exact delete, ambiguous copy/PUT landing, envelope reuse after a later miss, missing precommit/meta reconciliation, fence loss, and wrong-content publication; three witnesses prove the safe paths are reachable |
| `CaIncarnationCore.tla` | `INV_NO_DANGLE`, `INV_NO_LOSS`, `INV_NO_RETURN` — the safety spine for the whole GC core | `sab_unconddelete`: replacing the exact-token delete with an unconditional one lets a stale delete kill a replacement incarnation |
| `CaBuildRootPrecommit.tla` | `INV_NO_DANGLE_COMMITTED` — a committed manifest never references an absent blob | Reproduces the dangling-manifest hazard exactly: `WriteBlob → AdoptBlob → BuildDie → GcDelete → Commit` with no presence re-check publishes a manifest over a deleted blob |
| `CaGcLeaseCore.tla` | `NoFalseSteal` — no leader steals leadership from a live, mid-round incumbent | Without the advisory heartbeat, a frozen `seq` during a round looks identical to a dead leader, and a second leader steals from the alive one |
| `CaCasMountCore.tla` | Reclaim exclusivity for an expired mount | `sab_wallclockreclaim`: trusting the foreign mount body's wall-clock timestamp (instead of observing a stable token on the reclaimer's own monotonic clock) breaks exclusivity |
| `CaB140DangleMerge.tla` | `INV_NO_LOSS` across a GC lease handoff | Trim-before-durable: a fold cursor trimmed from in-memory state (not the durable snapshot) skips a live edge across a lease handoff, and the referenced blob is deleted while still live |
| `CaGcRootLocalPartManifestCore.tla` | `INV_NO_DANGLE` over the root-local part-manifest fold | `sab_lazyfenceunsafe`: reusing a stale parent fence position instead of a fresh all-shard fence dangles a live object |
| `CaGcShardIncarnationCore.tla` | `INV_NO_DANGLING` — safety of registry-free namespace discovery | `sab_pathkeyedcursor`: dropping the per-shard incarnation from the fold cursor reintroduces an ABA hazard on delete-then-recreate at the same path |
| `CaGcAckFloorZombie.tla` | `INV_NO_DANGLE` under two fully-interleaved GC leaders | `sab_eagerdelete`: a leader deleting its own fresh (not-yet-pending) graduations — the pre-amendment single-phase behavior — dangles when a deposed leader's pass overlaps a live one |
| `CaGcRoundDeferCore.tla` | `NoOverDelete` — a deferred round may skip a rebuild only when nothing destructive is pending | `sab_graduate_on_stale`: dropping the "an unfolded delta covers this blob" guard lets a deferred round delete a blob its own unread history still protects |
| `CaEdgeBeforeObserve.tla` | The writer/`GC` publish order is safe to simplify | `sab_late_edge`: allowing adoption before the precommit closure is durable (the pre-fix order) dangles |
| `CaGcCondemnMarkerGate.tla` | Graduation requires confirmed durable `Condemned` evidence | Swallowing a failed asynchronous condemn-marker write let a writer adopt a token a later graduation was about to delete |
| `CaRefTableSnapshotLogCore.tla` | Dense, per-life ref ids with an in-band `_ckpt` recovery frontier | `sab_scanistruth`: trusting a listing as the source of truth for "acked" reproduces the real production incident where a `LIST` omitted two already-durable, already-acknowledged ref entries |

## Soak and chaos: the empirical oracle {#soak-and-chaos}

Model-checking and the soak/chaos harness (`utils/ca-soak/`) catch different classes of error, and
the design leans on both rather than either alone. `TLA+` proves a protocol's constraints before a
line of `C++` exists — the two-coordinate namespace-incarnation proof and the build-root necessity
proof were both design decisions made this way. The soak, running two `ReplicatedMergeTree`
replicas against one shared pool under a seeded workload and a seeded fault injector, finds what an
idealized model necessarily abstracts away: the dangling-manifest hazard and the condemned-body replacement orphan were
both first observed live in `system.cas_log` during soak runs, before either got a focused model. Each
quiesced soak checkpoint cross-checks `SQL` results against a model oracle and runs
`clickhouse-disks ca-fsck` plus `ca-gc-dryrun`, asserting `dangling=0`.

The relationship runs in both directions: the historical resurrect-reupload orphan (`utils/ca-soak` scenario
S30, root-caused via `system.cas_log`) got a focused `TLA+` reproduction that proved the fix and
was then retired once a deterministic `gtest` (`CASGCLeak.ResurrectReplacedIncarnationReclaimed`)
covered the same scenario for less ongoing cost — the model did its job as a pre-implementation
gate and the regression coverage moved to the cheaper, faster tool. A model's proven-safe shape
also becomes the thing a later soak scenario is written to stress. The `0x1430c`
incident — a `LIST` that omitted two already-durable ref entries, caught live by an instrumented
probe rather than reproduced by brute-force enumeration — is the clearest example: it is what made
`sab_scanistruth` a permanent, named counterexample rather than a one-off incident report.

## What this buys a reader {#what-this-buys}

None of this proves the shipped `C++` is bug-free — a model proves its own abstraction, and several
entries in the index are annotated `MIXED` or `DRIFTED` where the concrete mechanism has moved on
from what a model checks, with the audit trail kept precisely so that gap is visible rather than
implied. What it does buy: every safety rule in the GC core has an explicit counterexample on
record for the world where that rule is missing, and the corpus is itself periodically re-audited
for faithfulness to the code — a model whose guarantee the code no longer needs is deleted rather
than kept as false comfort.

## Test coverage {#test-coverage}

The implementation was built test-first (TDD), and the coverage is correspondingly dense:

| Layer | Volume |
|---|---|
| Unit tests (`gtest`, `CAS*` suites) | ~1,900 test cases across ~130 files, covering formats, the write and read paths, the ref machinery, `GC`, recovery, and the backend contract |
| Integration tests | 10 dedicated `test_cas_*` suites (shared pools, `GC` on S3, sharded `GC`, relink replication, fault-injected `INSERT` recovery, member decommission, and more) |
| Stateless tests | dozens of dedicated `CAS` tests (pool integrity, leftovers, fsck, GC), in addition to the whole standard suite running on a `CAS`-default server (below) |

## The whole test suite, on CAS by default {#stateless-suite-on-cas}

Beyond the model corpus and the soak harness, the standard ClickHouse **stateless test suite runs
green with `CAS` as the default `MergeTree` storage**: dedicated CI lanes
(the `cas storage` and `cas s3 storage` job families — the latter covering ASan/TSan/MSan/UBSan
and ARM against a real S3-compatible store) run every stateless test against a server whose default disk is
a `CAS` pool. A small set of tests carries the `no-cas-storage` tag and is skipped in those lanes —
tests that exercise a mechanism a content-addressed disk deliberately does not have (for example,
`s3_plain` layouts or deliberately corrupted on-disk part chains). Everything else — the thousands
of tests that define what `MergeTree` is supposed to do — passes unchanged on top of `CAS`.
