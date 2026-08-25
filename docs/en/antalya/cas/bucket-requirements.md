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
| Native same-store copy when `staging_backend = s3` | `IObjectStorage::copyObject` with `ObjectStorageCopyMode::NativeOnly` | The first absent staged publication may copy its complete object without a client-side fallback |
| Exact-token delete | `Backend::deleteExact` | GC must delete only the incarnation it condemned, never a replacement |
| Ranged `GET` | `Backend::get` / `Backend::getStream` with a `Range` | Opening one column file of a part costs one bounded read, not a whole-object fetch |
| `LIST` with a resumable cursor | `Backend::list` | GC discovery and the orphan-manifest sweep page through the pool without a separate index |
| No versioning / no delete markers | probed by `runCapabilityProbe`; `created_delete_marker` on `DeleteOutcome` | A delete marker over a live key would break exact-token semantics — GC would archive instead of reclaim |
| `TOKEN ⟹ CONTENT` (a repeated token implies unchanged bytes) | standing requirement on every `Backend` implementation | Not probed — it cannot be tested cheaply. A backend that recycled tokens would serve stale manifests, i.e. wrong query results, not merely an inefficiency |

Bucket **versioning is not required** — in fact it must be **disabled** on the generation-token
dialect (see below), because a token-exact delete on a versioned bucket archives a noncurrent
generation instead of reclaiming storage, silently stopping GC reclamation.

On the generation-token dialect that requirement is checked, and checked strictly: a writable mount
proceeds only when the probe *confirms* versioning is disabled. A bucket reported as versioned and a
probe that could not answer — the credential may not read the bucket's versioning configuration, or
the backend cannot report it — both refuse the mount. `CAS` does not assume the safe answer, because
the failure it would be assuming away is `GC` deleting objects it believes it reclaimed.

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
