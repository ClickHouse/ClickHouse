---
description: 'The object-store contract a bucket must satisfy to host content-addressed storage, and which providers qualify.'
sidebar_label: 'Bucket requirements'
sidebar_position: 4
slug: /antalya/cas/bucket-requirements
title: 'CAS Bucket Requirements'
doc_type: 'reference'
---

# Bucket requirements {#bucket-requirements}

`CAS` is built on a small object-store contract (`Backend/CasBackend.h`), checked by a capability
probe that runs at every writable mount and fails closed: an object store that does not enforce
these conditions is refused rather than trusted.

## The capability table {#capability-table}

| Requirement | Interface method | Why it is needed |
|---|---|---|
| Read-after-write on a fresh key | `Backend::get` / `Backend::head` | Recovery listings and point reads must see what was just written |
| Conditional create (`If-None-Match: *`) | `Backend::putIfAbsent`, `Backend::casPut` with expected absence | Write-once creation of manifests and control/log objects; blob bodies use unconditional publication after `HEAD` |
| Conditional overwrite (`If-Match: <token>`) | `Backend::putOverwrite`, `Backend::casPut` | The one mutual-exclusion primitive: mount leases, `gc/state` |
| Unconditional complete-object publication | `Backend::publishBlob` | An absent or condemned content-addressed body is replaced atomically; native stores may use multipart |
| Native same-store copy when `cas_staging_backend = s3` | `IObjectStorage::copyObject` with `ObjectStorageCopyMode::NativeOnly` | The first absent staged publication may copy its complete object without a client-side fallback |
| Exact-token delete | `Backend::deleteExact` | GC must delete only the incarnation it condemned, never a replacement |
| Ranged `GET` | `Backend::get` / `Backend::getStream` with a `Range` | Opening one column file of a part costs one bounded read, not a whole-object fetch |
| `LIST` with a resumable cursor | `Backend::list` | GC discovery and the orphan-manifest sweep page through the pool without a separate index |
| No versioning / no delete markers | probed by `runCapabilityProbe`; `created_delete_marker` on `DeleteOutcome` | A delete marker over a live key would break exact-token semantics — GC would archive instead of reclaim |
| `TOKEN ⟹ CONTENT` (a repeated token implies unchanged bytes) | standing requirement on every `Backend` implementation | Not probed — it cannot be tested cheaply. A backend that recycled tokens would serve stale manifests, i.e. wrong query results, not merely an inefficiency |

Bucket **versioning is not required** — in fact it must be **disabled** on the generation-token
dialect (see below), because a token-exact delete on a versioned bucket archives a noncurrent
generation instead of reclaiming storage, silently stopping GC reclamation.

On the generation-token dialect that requirement is checked at mount. A bucket reported as versioned
refuses the mount. A probe that could not answer — the credential may not read the bucket's versioning
configuration (`storage.buckets.get` on GCS), or the backend cannot report it — does not: the mount
proceeds and logs a warning naming what it could not verify, because an unreadable configuration is
not evidence of a versioned bucket, and refusing on it would turn a missing IAM grant into an outage.
In that case confirming that versioning is disabled is your responsibility, exactly as soft delete is
below; grant the permission if you want the mount to verify it for you.

Because that check is part of the mount battery, `skip_access_check = true` is refused on a writable
generation-token disk. Mount the disk read-only if you need to start before the access check can
pass.

## Soft delete is an operator precondition {#soft-delete}

Object **soft delete must be disabled** on a `CAS` bucket, and unlike versioning this one is *not*
verified at mount. Google Cloud Storage exposes the soft-delete policy through its JSON API, while
this backend and both of its authentication modes speak the XML API, so the storage path `CAS` uses
cannot inspect it. Disabling it is therefore your responsibility, not something a successful mount
attests to.

Soft delete does not leave the deleted generation live, so it does not break exact-token semantics
the way versioning does. What it does is delay physical reclamation until the retention period
expires: `GC` reports space as reclaimed while the bill still reflects it.

## Request rate, and the limit that is not the one you expect {#request-rate}

Google Cloud Storage publishes two kinds of ceiling, and the one that constrains `CAS` is the
smaller and less-known of them.

A bucket starts at roughly **1000 object writes per second** — uploads, updates and deletes — and
roughly **5000 object reads per second**, counting listings and metadata reads as reads. Those
ceilings are not fixed: Cloud Storage raises them by splitting the index range behind the bucket,
which it says takes "on the order of minutes" to detect and act on. Buckets with a hierarchical
namespace start up to eight times higher.

Separately, Cloud Storage applies **a much smaller limit to repeated writes to the same object
name**. Google documents that this limit exists but does not publish its value. Measured against a
live bucket from this codebase, it begins to bite at approximately one mutation per second on a
single key, and it does not participate in the auto-scaling above — splitting an index range cannot
help a single name.

That second limit is the one `CAS` meets first, because two of its objects are single fixed names
written on a hot path:

| Object | One per | Written on |
|---|---|---|
| `cas/ns/state/<namespace>/_ckpt` | table | every durable ref-log transaction, plus namespace birth, epoch seal and snapshot |
| `cas/ref_catalog` | pool | twice per `CREATE TABLE` and twice per `DROP TABLE` |

Blob bodies and their metadata sidecars are named by content hash and are therefore spread the way
Google's own guidance asks for: it recommends "completely random object names" for the best load
distribution, and a hashed prefix where names would otherwise be sequential. Ref-log transactions are
sequential within a namespace but are written under a per-namespace prefix, so they scale with the
number of tables rather than sharing one index range.

### What this means for a deployment {#rate-consequences}

- **A single table commits at about one transaction per second** on Google Cloud Storage. Inserts,
  merges and mutations on that table queue behind the checkpoint write; they do not fail, but the
  lane's throughput is capped and each flush's tail takes longer than it would on a store without
  the per-name limit.
- **A pool performs about one table lifecycle transition per second.** Concurrent `CREATE TABLE` or
  `DROP TABLE` beyond that rate contends on the pool-wide catalog. Test suites that create hundreds
  of tables in parallel are the case that provokes this; ordinary production DDL is not.
- **Ramping up gradually is Google's documented expectation.** Its guidance is to increase the
  request rate "no faster than doubling the rate over a period of 20 minutes", and to pause or
  reduce the rate when latency or error rates rise. A pool that goes from idle to full write load in
  one step will see throttling before the bucket has redistributed the load.

### Throttling is a retryable condition, not a failure {#rate-errors}

Cloud Storage signals a rate it will not serve with HTTP `429`, `408`, or a `5xx` status, and its
retry guidance names all three, together with socket timeouts and TCP disconnects, as retryable with
exponential backoff and jitter. Every mutable-object write `CAS` issues carries a generation
precondition, which places it in Google's *conditionally idempotent* class — a retry either applies
exactly once or fails the precondition, never applies twice. Retrying them is therefore safe by
Google's own rule, not merely by ours.

### Reads over a wide-area link want a cache disk {#rate-reads}

The read ceiling is high enough that `CAS` does not approach it, but latency is a separate matter.
A cacheless `CAS` disk pays a round trip per column file per part: measured against a bucket in
another region, a `SELECT` issued about 725 ranged reads and took 3.6 seconds at the median and 15.7
seconds at the ninety-ninth percentile. Put a `cache` disk in front of the `CAS` disk for any
deployment where the bucket is not local to the server.

## Platform support {#platform-support}

The deterministic request-construction coverage is green, but the
[real-GCS release gate](/superpowers/cas/unconditional-blob-publication-live-results) remains blocked
until its credentialed OAuth and HMAC groups run against Google Cloud Storage. A fake service cannot
establish acceptance of Google's multipart, native-copy, and exact-delete wire behavior.

| Platform | Status | Notes |
|---|---|---|
| AWS S3 | ✓ | Native `ETag`-based conditional dialect for mutable objects and exact deletion; blob publication is unconditional |
| Google Cloud Storage | implementation complete; release gate pending | Generation-token dialect for mutable objects/native-token `HEAD`/exact deletion, opted into via `http_client = gcs_hmac` or `gcp_oauth`; blob publication uses ordinary copy/multipart. Real credentialed GCS groups have not run yet |
| Azure Blob Storage | probably | Azure's REST API documents the equivalent conditional headers, but ClickHouse's Azure object-storage backend does not yet wire up a `CAS` conditional dialect the way the S3 and GCS paths do — untested, not validated by the capability probe |
| Other S3-compatible stores | only with enforced conditional operations | The capability probe is the actual gate: a store that silently ignores `If-None-Match`/`If-Match` (accepting and applying the write regardless) fails the probe and is refused. `RustFS` passes the full battery and is used as the project's test backend; `Garage` was evaluated and rejected because it silently ignores conditional operations |

The full mechanics of the two dialects — how the backend detects which one a given endpoint speaks,
what the capability probe actually checks, and how exact-token deletes map onto each provider's
primitives — are in [the Backend architecture page](/antalya/cas/architecture/backend).
