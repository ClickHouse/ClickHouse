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
| Conditional create (`If-None-Match: *`) | `Backend::putIfAbsent`, `Backend::putIfAbsentStream` | Write-once creation of blobs, manifests, and ref-log entries |
| Conditional overwrite (`If-Match: <token>`) | `Backend::putOverwrite`, `Backend::casPut` | The one mutual-exclusion primitive: mount leases, `gc/state` |
| Exact-token delete | `Backend::deleteExact` | GC must delete only the incarnation it condemned, never a resurrected replacement |
| Ranged `GET` | `Backend::get` / `Backend::getStream` with a `Range` | Opening one column file of a part costs one bounded read, not a whole-object fetch |
| `LIST` with a resumable cursor | `Backend::list` | GC discovery and the orphan-manifest sweep page through the pool without a separate index |
| No versioning / no delete markers | probed by `runCapabilityProbe`; `created_delete_marker` on `DeleteOutcome` | A delete marker over a live key would break exact-token semantics — GC would archive instead of reclaim |
| `TOKEN ⟹ CONTENT` (a repeated token implies unchanged bytes) | standing requirement on every `Backend` implementation | Not probed — it cannot be tested cheaply. A backend that recycled tokens would serve stale manifests, i.e. wrong query results, not merely an inefficiency |

Bucket **versioning is not required** — in fact it must be **disabled** on the generation-token
dialect (see below), because a token-exact delete on a versioned bucket archives a noncurrent
generation instead of reclaiming storage, silently stopping GC reclamation.

## Platform support {#platform-support}

| Platform | Status | Notes |
|---|---|---|
| AWS S3 | ✓ | Native `ETag`-based conditional dialect: `If-None-Match` / `If-Match` used directly |
| Google Cloud Storage | ✓ | Generation-token dialect: conditional headers are rewritten to `x-goog-if-generation-match`, opted into via `http_client = gcs_hmac` or `gcp_oauth` |
| Azure Blob Storage | probably | Azure's REST API documents the equivalent conditional headers, but ClickHouse's Azure object-storage backend does not yet wire up a `CAS` conditional dialect the way the S3 and GCS paths do — untested, not validated by the capability probe |
| Other S3-compatible stores | only with enforced conditional operations | The capability probe is the actual gate: a store that silently ignores `If-None-Match`/`If-Match` (accepting and applying the write regardless) fails the probe and is refused. `RustFS` passes the full battery and is used as the project's test backend; `Garage` was evaluated and rejected because it silently ignores conditional operations |

The full mechanics of the two dialects — how the backend detects which one a given endpoint speaks,
what the capability probe actually checks, and how exact-token deletes map onto each provider's
primitives — are in [the Backend architecture page](/antalya/cas/architecture/backend).
