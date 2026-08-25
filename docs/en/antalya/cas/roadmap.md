---
description: 'What CAS ships today, what is still planned, known platform limitations, and design directions deliberately not taken.'
sidebar_label: 'Roadmap'
sidebar_position: 5
slug: /antalya/cas/roadmap
title: 'CAS Roadmap'
doc_type: 'guide'
---

# CAS roadmap {#cas-roadmap}

CAS is experimental (see [status](/antalya/cas/)): the format and SQL surface can still change.
This page tracks what already works, what is still ahead, and — since a project this deep in
adversarial verification collects real dead ends — what was tried and deliberately not shipped.

## Shipped {#shipped}

**Storage and object model.** Content-addressed blobs deduplicated across every replica sharing
a pool; immutable part manifests; a pluggable blob-hash algorithm (`cityhash128` default,
`xxh3-128`, or `sha256`) fixed per pool at creation; a JSON-text object format end to end (no
binary framing, no protobuf) so any object can be read with ordinary line-oriented tools.

**Write path.** Conditional writes for mutable metadata/control objects; mandatory blob `HEAD`
followed by adoption or unconditional, multipart-capable publication; a bounded thread pool
fanning out multi-blob part materialization in parallel; carry-forward on mutation for `Wide` parts (an
untouched column is re-referenced, not re-hashed).

**Read path.** Ref resolution to manifest to ranged blob reads, with a manifest-decode cache and
a part-folder view cache sitting on that path.

**Replication.** Fetch by relink between replicas sharing a pool — a replicated fetch publishes a
ref pointing at blobs the pool already has, at zero bytes on the wire — with a publish-then-confirm
protocol that closes the sender-crash and stale-cache races a naive relink would be exposed to.

**Garbage collection.** An 18-phase round built on a causal ack-floor (no separate fence-and-recheck
phase); sharded folding (`cas_gc_shards`); condemn/spare bookkeeping; generation pruning with a
configurable retention window; a dry-run mode and a rebuild path for recovery.

**Mounts and identity.** Explicit `cas_server_root_id` per disk; a renewable mount lease with
observation-based reclaim of an expired predecessor (never trusting a foreign body's wall-clock
timestamp); clean decommission of a permanently departed pool member
(`SYSTEM CAS DROP POOL MEMBER`).

**Backends.** AWS S3 (`ETag`-based conditional dialect) is validated. Google Cloud Storage's
generation-token implementation and deterministic tests are complete, but its credentialed
[real-GCS release gate](/superpowers/cas/unconditional-blob-publication-live-results) has not run. A
capability probe runs at every writable mount and refuses a backend that does not enforce the
conditions CAS depends on.

**Operability.** `system.cas_log`, `system.cas_gc_log`, and `system.cas_mounts` for introspection;
`clickhouse-disks` commands `ca-fsck`, `ca-inspect`, `ca-gc-dryrun`, and `ca-gc-rebuild`; the
`SYSTEM CAS` SQL control surface (`GC RUN`/`STOP`/`START`/`REBUILD`, `FSCK`, `FORGET`, `DROP POOL
MEMBER`).

**Coexistence.** `metadata_type = cas` is opt-in per disk; zero-copy replication keeps working
unmodified on disks that do not opt in — see [why CAS exists](/antalya/cas/) for the fuller
positioning.

## In progress / planned {#in-progress}

- **GCS and Azure real-store validation.** The new GCS publication groups still require credentials;
  Azure has no wired CAS conditional dialect. See [known limitations](#known-limitations) below.
- **WORM deployments.** A read-only disk mode exists today; a fuller write-once story — a pool
  served immutably, with pinned snapshots for read-only replicas — has a draft design and is not
  yet implemented.
- **Backup and restore.** See [Backups](#backups) below — this is further along as a design than as
  an implementation.
- **First-class local-disk pools.** Today a pool over local paths runs a minimal best-effort
  emulation of the token-conditional mutable-object dialect. Blob publication materializes a whole
  object and is serialized to retain a one-body memory bound. Making the
  local mode efficient in its own right is under consideration: a local CAS tier is a natural target
  for backups, pinned snapshots, and moving data between CAS tiers.

## Known limitations {#known-limitations}

- **Azure Blob Storage's REST API documents the equivalent conditional headers CAS needs, but no
  CAS conditional-write dialect is wired up for it yet** — untested, not validated by the capability
  probe. See [bucket requirements](/antalya/cas/bucket-requirements) and
  [the backend page](/antalya/cas/architecture/backend) for the AWS/GCS dialects that are wired.
- **Other S3-compatible object stores qualify only if they pass the capability probe** — a store
  that silently ignores conditional writes is refused at mount time rather than trusted. Bucket
  versioning must be off; it is not required to be on.
- **An `encrypted` disk wrapping a `CAS` disk is not supported yet** — `CREATE TABLE` on such a
  disk succeeds, but the first `INSERT` fails (`Autocommit writes are not supported for content part
  files on a content-addressed disk`). Layering a `cache` disk in front of `CAS` is the supported
  wrapper shape (see [configuration](/antalya/cas/configuration)); encryption at rest currently has
  to come from the object store side.
- **The format and settings surface can still change.** CAS is pre-release: there is no persisted
  production data to keep compatible, so a format change costs a version bump, not a migration.
  Treat every detail on these pages as subject to change until the format is declared stable.

## Backups {#backups}

A `snapshot` / `mirror` / `fetch` / `restore` design is **approved but not implemented**. The
model is deliberately git-shaped: `snapshot` is instant and free (like `git tag` — it references
existing manifests, copies nothing); `mirror` is a continuous pull from a production pool into a
backup pool (like `git push --mirror`); `fetch` is a selective pull from a backup pool into a
fresh pool (a partial clone); `restore` is an in-pool relink (like `git checkout`, instant). One
closure-walk-and-hash-verification primitive is meant to serve all three pool-to-pool movements.
None of this is wired into the `BACKUP`/`RESTORE` SQL surface yet.

## Deliberately rejected directions {#rejected}

A short pointer list; the reasons and the counterexamples that drove each decision are in
[design history](/antalya/cas/architecture/design-history).

- A Merkle tree layer as a distinct object kind.
- Epoch-based reclamation as the GC core.
- An integer in-degree refcount instead of a folded edge set.
- A persistent, append-only namespace registry for GC discovery.
- Per-incarnation body keys as an alternative to an in-body incarnation tag.
- Using a blob's freshness metadata as the authority for its lifecycle instead of an advisory hint.
- A separate all-shard fence-and-recheck phase per GC round.
- A sparse ref-id allocator with a certificate stack bolted on to prove completeness.
- Extending zero-copy replication instead of building a new mechanism — CAS is an alternative to
  zero-copy, not a replacement; both remain available.
