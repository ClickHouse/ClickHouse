---
description: 'A condensed record of the paths CAS explored and rejected, and the major design pivots that produced the current architecture.'
sidebar_label: 'Design history'
sidebar_position: 13
slug: /antalya/cas/architecture/design-history
title: 'CAS Architecture — Design History'
doc_type: 'reference'
---

# CAS architecture — design history {#design-history}

This page is a condensed record of the roads not taken: what was tried, why it was abandoned, and
the sequence of pivots that produced the architecture described elsewhere in this section.

## Rejected paths {#rejected-paths}

| What it was | Why it was abandoned | What replaced it |
|---|---|---|
| **Generation-in-the-key** (Epoch-Based Reclamation core; blob keys carried a generation, `blobs/<hash>/<gen>`) | Required `O(files)` persistent `Keeper` writes per commit and colliding intent keys across writers building identical content; a stuck writer stalled reclamation pool-wide | The incarnation-token design: identity moved into the object body and delete precision into the backend token, removing the generation from every key |
| **Merkle tree layer** (a `Tree` object kind, `trees/<hash>` prefix, `child_gen` carried inside a tree's own identity) | Depended on the generation-in-the-key core: a reclaim at any child propagated a new generation up the entire tree chain, and the tree layer was itself an extra surface for the same class of bug | Removed entirely; trees became manifest-internal, and `Blob` is the sole durable object kind besides the manifest and the ref |
| **Integer in-degree refcount** (a mutable counter, incremented per reference, decremented per release) | The decide-to-reference-then-not-yet-durable window let the fold observe in-degree 0 for a still-live blob; a mutable counter also costs a `CAS` round-trip proportional to write volume | A derived count: `GC` folds a multiset of `+`/`-` source-edge deltas, so losing or duplicating a record can only delay reclamation, never accelerate one |
| **Extending zero-copy replication instead of a new mechanism** | Zero-copy's structural costs (a commit spanning local disk, S3, and `Keeper`; a mutable refcount) are inherent to its design, not a bug to patch | `CAS` is an alternative to zero-copy, not a replacement: both remain available, `metadata_type = cas` is opt-in per disk, and no existing deployment needs to migrate |
| **Per-incarnation body keys** (`blobs/xx/<hash>.<incarnation>`, an alternative to the in-body incarnation tag) | A resurrect reusing the condemned incarnation instead of minting a fresh one reintroduced the shared-key race; structurally this was generation-in-the-key again | The in-body `incarnation_tag` plus exact-token body delete, which keeps the generation out of every object key |
| **Meta as the lifecycle linearizer** (a per-hash `.meta` object whose presence/absence *was* the authority for a blob's lifetime) | The marker is a point-read hint only, never consulted by reads; treating it as the linearizer would assert a guarantee the design does not make | The meta stays advisory: the in-body incarnation tag and exact-token delete are the real authority, and an absent meta reads identically to `Clean` |
| **Raw immutable bodies with a three-state tombstone meta** | A resurrect displacing the body forced a terminal-tombstone handshake — a writer↔`GC` liveness coupling that could re-enable data loss | The settled one-key-per-hash design with an in-body incarnation tag |
| **Conditional blob creation and conditional staged copy** | Coupled immutable payload publication to provider-specific ETag/generation responses, forced generation-token GCS blobs into a single-`PUT` size cliff, and split the writer into provider/source-specific branches | One provider-neutral state machine: durable precommit, mandatory blob `HEAD`, unconditional publication for absent or condemned bodies, freshness-metadata reconciliation, and explicit `Materialized`/`TrustedManifest` proof |
| **A persistent, append-only namespace registry for `GC` discovery** | Never deregistered on drop, so it grew monotonically forever; its fence cost scaled with namespaces ever created, not namespaces live | Discovery from the ref data itself, made safe by two independent coordinates: a durable per-shard incarnation plus a pool-global round |
| **A separate all-shard fence-and-recheck phase per `GC` round** | Both phases cost `O(pool size)` GET+CAS every round regardless of churn — roughly 2.4 million requests at 100k tables | A causal ack-floor: one streaming merge per round, with no separate fence or recheck phase, cutting the request count by roughly three orders of magnitude |
| **A pool-wide sparse ref-id allocator with completeness certificates bolted on** | Successive additive fixes kept growing without closing the root cause: absence is undecidable in a sparse id space | The invariants were changed instead of patched: dense per-life ids derived from applied state, an in-band epoch seal, and a `_ckpt` head object carrying the exact acknowledged frontier |

## Turns at a glance {#turns-at-a-glance}

| Date | Turn |
|---|---|
| 2026-06-01 | Starting point: "content-addressed storage for `MergeTree`" thesis and a working proof of concept |
| 2026-06-07 – 10 | The generation-in-the-key core is abandoned; the incarnation-token design replaces it |
| 2026-06-11 | The incarnation model passes exhaustive model checking with zero violations |
| 2026-06-18 | A dangling manifest reference — a committed manifest naming an already-deleted blob — leads to replacing per-blob protection hints with structural build-root reachability |
| 2026-06-24 – 26 | Formats begin converging on a single self-describing envelope (completed in July as the all-text, JSON-based codec set) |
| 2026-06-26 | Root-local full-tree manifests collapse a forest of small `GC` objects into one hot/cold split |
| 2026-07-01 | The namespace registry is deleted; discovery moves to the two-coordinate incarnation-and-round scheme |
| 2026-07-02 | Fence-and-recheck `GC` rounds are replaced by the one-pass, causal ack-floor round |
| 2026-07-06 – 10 | Writer/`GC` simplification: promote-time revalidation of tokened dependencies is proved redundant |
| 2026-07-13 | Mount-lease handover becomes boundary-exclusive, closing the cross-epoch grace window without a timeout |
| 2026-07-15 | All part files become content-addressed: the mutable file set drops to empty, and disk-transaction dispatch collapses to one precommit contract |
| 2026-07-17 | An acknowledged `INSERT` that could be lost is traced to a removed durability guard and fixed |
| 2026-07-26 | A `LIST` omitting two already-durable ref entries is caught during a soak run — the incident that settles the trust model for listings |
| 2026-07-27 – 29 | The sparse-id certificate stack is abandoned; the invariants change instead — dense ids, an in-band epoch seal, and a `_ckpt` recovery frontier |
| 2026-08-01 – 03 | Recovery stops reading listings entirely and works from authoritative objects; the listing trust model is finalized |
| 2026-08-21 – 23 | Blob creation stops using conditional PUT/copy; the focused `CaBlobPublishCore` model gates mandatory `HEAD` followed by unconditional, multipart-capable publication |

## The pattern underneath {#the-pattern}

A few reflexes recur across these pivots and still apply to new design work:

- **Re-derive the invariant, don't patch the mechanism.** Every durable fix came from asking what
  property must hold, not from patching the specific failure observed.
- **Delay is acceptable, authorization is not.** A stale-but-honest observation can only ever
  postpone a decision; the design consistently rejects any mechanism that could *accelerate* a
  destructive action past its safety gates — a delay is a latency cost, a wrongful authorization is
  data loss.
- **A model that no longer matches the code is worse than no model.** Superseded models are
  removed rather than kept: an unfaithful proof is false comfort, not documentation.
