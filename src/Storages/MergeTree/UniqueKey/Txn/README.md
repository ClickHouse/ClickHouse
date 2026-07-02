# UNIQUE KEY transaction layer

Per-partition snapshot-isolated reads and synchronous writes for UNIQUE KEY `MergeTree`, over
versioned per-part delete bitmaps. One non-replicated ("Local") implementation today, behind strategy
interfaces so a shared/replicated backend can replace it without touching the protocol.

In this slice only `DELETE` drives a write commit (`PartitionTxnController::commit` is wired from
`executeUniqueKeyDelete`); the `INSERT`-upsert commit is a later PR. The commit protocol below is
written to drive both, so it describes the `INSERT` path as the intended design — but no `INSERT`
caller exists yet.

## Core idea

Each partition has a monotonic **CSN** (commit sequence number). A write commits at one fresh CSN by
doing two things atomically:

- **publish a new part** — `INSERT` publishes the data part it just wrote; `DELETE` publishes a
  lightweight *marker* part. Both go visible via `rename(tmp → active)` (the same primitive).
- **install cumulative delete bitmaps** on the older parts it touches — for each touched part it
  writes a new bitmap `prev ∪ new_kills` at the fresh CSN. `DELETE` kills the rows it matched;
  `INSERT` (upsert) kills the rows in older parts whose keys the new part supersedes. A write that
  touches no older part (e.g. an `INSERT` of all-new keys) just publishes its part.

A reader pins a CSN and, per part, sees the bitmap with the largest `csn ≤ pinned` — and only the
parts whose `creation_csn ≤ pinned` — so a concurrent write (its new part *and* its kills together)
is invisible to an in-flight query. Everything else exists to make the publish atomic and crash-safe.

**Invariants (do not break)**

- **CSN monotonic per part** — `installBitmap` rejects a csn not strictly above the latest, so a
  rollback can only ever unlink the current commit's own write.
- **Cumulative** — every bitmap version is a superset of its predecessor.
- **Snapshot visibility by intrinsic `creation_csn`** — a reader pins one csn `C` and its GC pin
  atomically (under the coordinator lock, so GC can't reclaim a bitmap the reader still needs), then
  sees only parts with `creation_csn ≤ C`, each with the bitmap of max `csn ≤ C`. Part visibility is
  a **per-part predicate on the part's intrinsic manifest `creation_csn`** — NOT a part list captured
  at a special moment (its capture timing is irrelevant to consistency, which matters because
  `getParts()` and the csn pin take different locks and can't be captured atomically), and NOT a
  `snapshot->parts` set-intersect (which would drop a merge-retired part `≤ C` that an in-flight
  SELECT still legitimately reads). A part newer than `C` never gets `C`-era delete bitmaps applied.
- **Manifest before bitmaps** — fsync'd first, so recovery always finds what a commit claimed.
- **Fail-closed** — an unrecoverable rollback latches the partition rather than risk surfacing an
  orphaned bitmap as committed.

## Reads — snapshot

```
                     csn3         csn7         csn9
                     ●────────────●────────────●────────▶ CSN
snapshot = 8 ─────────────────────┘ picks csn7              ── IBitmapStore::readBitmap(part, 8)
                                    (csn9 = newer write → invisible)

SELECT / count
   │
   ▼ takeQuerySnapshot()                                    ── PartitionTxnController::takeQuerySnapshot
   ├─▶ pin csn C + GC floor, atomically under the lock      ── IPinRegistry::acquire
   └─▶ per part:
          ├─ skip if creation_csn > C (newer than snapshot) ── isPartVisibleAtSnapshotCsn
          └─ else bitmap with max csn ≤ C                   ── IBitmapStore::readBitmap
                 └─ granule-skip + row-filter drop dead rows
```

## Writes — atomic commit at a fresh CSN

The same `commit` drives `INSERT` and `DELETE`. The request carries a `staged` part to publish
(`INSERT` → data part, `DELETE` → marker part) and a `touched` list of older parts with their kill
deltas (`INSERT` → superseded duplicate keys, `DELETE` → matched rows; either may be empty).

A whole write statement is serialized per partition by the WRITER lock, held by the caller across
`resolve → stage → commit`. Without it, two concurrent writers could each read `prev` before either
publishes and the later would overwrite the earlier's kills (lost update). It is the OUTER lock and
distinct from the publish lock `attemptCommit` takes internally; readers never take it.

- **Local** (`LocalCommitCoordinator`) — `lockForWrite()` returns a held per-partition `writer_mutex`
  (pessimistic). Lock order: `writer_mutex` (outer) → publish `commit_lock` (inner).
- **Shared** (future, Keeper-CAS) — `lockForWrite()` returns an empty (unlocked) guard; serialization
  is the CAS retry loop in `commit`, not a process-local mutex.

```
                                                            ── controller.lockForWrite()  (caller holds across the statement)
INSERT (data part)                 DELETE (marker part)        Local: per-partition writer_mutex
        │                                  │                   Shared: empty guard (CAS retry serializes)
        └────────────────┬────────────────┘
                         ▼
   commit()                                                 ── PartitionTxnController::commit
   ├─ snap = readSnapshot()                                 ── ICommitCoordinator::readSnapshot
   ├─ per touched part:
   │     prev = readBitmap(part, snap.csn)                  ── IBitmapStore::readBitmap
   │     cumulative = prev ∪ new_kills
   └─ attemptCommit(publish)                                ── ICommitCoordinator::attemptCommit
        ┌── under the publish lock, fresh csn C ───────────────────────────────────────────┐
        │  1. finalize manifest, fsync                      ── UniqueKeyManifest::write    │
        │  2. installBitmap(part, C, …) per touched         ── IBitmapStore::installBitmap │
        │  3. rename staged tmp→active ; partition.csn = C  ── publish callback            │
        └──────────────────────────────────────────────────────────────────────────────────┘
        throw → roll back the installs done so far          ── IBitmapStore::removeBitmap
                rollback itself fails → latch the partition FAIL-CLOSED

crash mid-commit  ── at table load ──                       ── MergeTreeData::runUniqueKeyTxnRecovery
   read each tmp_* manifest, unlink the bitmaps of any commit that never reached step 3
```

Steps 1–3 share the one CSN, so the manifest, the sidecars it names, and the published part agree.
