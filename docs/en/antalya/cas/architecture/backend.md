---
description: 'The Cas::Backend storage seam, its token contract, the per-provider conditional-write dialects, and the mount-time capability probe.'
sidebar_label: 'Backend abstraction'
sidebar_position: 11
slug: /antalya/cas/architecture/backend
title: 'CAS Architecture — Backend Abstraction'
doc_type: 'reference'
---

# Backend abstraction {#backend-abstraction}

Every protocol described elsewhere in this set — blobs, manifests, refs, mounts, GC — is written
against one interface, `Cas::Backend` (`Backend/CasBackend.h`). It is a token-aware storage seam:
every present key has exactly one current incarnation identified by an opaque `Token`, and
`putOverwrite`/`casPut` succeed only against the expected current token (or expected absence).

## The interface {#interface}

| Method | Contract |
|---|---|
| `get` / `getStream` | Read bytes (or a forward-only stream, for write-once objects) plus the token of the incarnation read |
| `head` | Existence, size, token, and metadata without reading the body |
| `putIfAbsent` / `putIfAbsentStream` | Create-if-absent (`If-None-Match: *`); `PreconditionFailed` is a returned outcome, never an exception |
| `putOverwrite` | Replace the current object only when its token equals `expected`; a mismatch is a returned outcome |
| `casPut` | `expected == nullopt` ⇒ create-if-absent CAS (used for the first write of a root object); a set `expected` conditionally replaces that exact incarnation |
| `deleteExact` | Delete only the incarnation named by `token`; a token mismatch (`TokenMismatch`) leaves the object untouched and is distinguished from `NotFound` |
| `list` | One page of keys under a prefix, resumed by the backend's own cursor |
| `supportsListTokens` | Whether `list` can surface a per-key incarnation token, letting GC discovery skip an unchanged root shard without a `GET` |
| `promoteStaged` / `resurrect` | `promoteStaged`: write-once server-side copy from S3 staging (optional, defaults to `NOT_IMPLEMENTED`). `resurrect`: unconditional re-upload displacing a condemned incarnation from a caller-supplied reader (streamed on remote object storage, materialized one-at-a-time on the local emulated mode); size-checked before publication, and fresh-tagged so pending deletes of the old incarnation cannot remove it |

`deleteExact`, `putIfAbsent`/`putIfAbsentStream`, and `putOverwrite`/`casPut` are safety-critical:
they are what makes exact-token deletes, write-once creation, and mutual exclusion hold. Every
other method is protocol hygiene.

**`TOKEN ⟹ CONTENT`** is the one contract item the capability probe cannot check: a token must
uniquely identify the byte content of the incarnation it labels, so that a repeated token never
means different bytes. The read-path decode cache skips a re-read on a token match, so a backend
that recycled tokens across different content would serve stale manifests — a wrong-result bug, not
merely inefficiency. `S3` `ETag`s are content-derived; the in-memory and emulated backends mint a
strictly monotonic sequence that is never reused. This remains a standing requirement of every
backend implementation, not a property the probe verifies.

## Provider dialects {#dialects}

`ObjectStorageBackend` (`Backend/CasObjectStorageBackend.cpp`) wraps one `IObjectStorage` and picks
its token dialect from `IObjectStorage::conditionalOpsUseGenerationTokens()`:

| Dialect | Token type | How a conditional write is expressed |
|---|---|---|
| `AWS` (default) | `ETag` | `If-None-Match: *` / `If-Match: <etag>` sent as-is |
| `GCS` | `Generation` | The backend rewrites conditional headers before the request goes out: `If-None-Match: *` becomes `x-goog-if-generation-match: 0`, and `If-Match: <etag>` becomes `x-goog-if-generation-match: <generation>` (`applyGcsConditionalDialectToRequest`, `IO/S3/GCSConditionalDialect.cpp`) |

The GCS dialect is opted into by client configuration (`http_client = gcs_hmac` or `gcp_oauth`), not
auto-detected from the endpoint host. It also rejects one shape outright: a **conditional
`CompleteMultipartUpload`** throws rather than silently dropping the precondition, because GCS
ignores preconditions on that call — a measured, documented gap, not a hypothetical one. `CAS`'s
conditional writes therefore always take the single-`PUT` path on a generation-dialect backend.

This bounds conditional writes only: the write-once create is therefore single-part and limited by
`gcs_max_conditional_put_bytes` on a generation dialect, while the unconditional resurrect takes the
ordinary multipart path and has no size limit on any backend.

Every request carrying a rewritten header also has its AWS auth headers stripped and every
remaining `x-amz-*` header renamed to `x-goog-*`, since GCS rejects a mixed header set.

Azure Blob Storage's REST API documents equivalent conditional headers (`If-None-Match`,
`If-Match`), but no third dialect exists in this backend yet — `IObjectStorage`'s Azure
implementation does not currently wire up a `CAS` conditional path, so Azure is untested by the
capability probe below, not merely a slower-verified third case.

## Exact-token delete, per provider {#exact-token-delete}

`deleteExact(key, token)` is realized as a conditional `DELETE` naming the token as a precondition:
an `If-Match`-style delete on `AWS` (`ETag`), a generation-match delete on `GCS`. A precondition
failure — `S3::isPreconditionFailedError` — is reported as `DeleteOutcome::TokenMismatch`, never as
an exception; the object is left untouched. `DeleteOutcome::created_delete_marker` reports whether
the backend created a delete marker instead of actually removing the object, which the capability
probe rejects: a bucket with versioning enabled would let `CAS` "delete" a blob without freeing any
storage, and GC would silently stop reclaiming.

## The capability probe {#capability-probe}

`runCapabilityProbe` (`Backend/CasProbe.cpp`) runs a throwaway-key battery against every writable
mount, described in full on the [bucket requirements](/antalya/cas/bucket-requirements) page. It is
fail-closed: any check that does not pass throws `NOT_IMPLEMENTED` naming the specific failure, and
the mount refuses to become writable. Two mount-time gates sit alongside it:

- `checkPoolPreconditions` — on the `GCS`-dialect combination only, verifies bucket versioning is
  off (a confirmed `Enabled` throws; an inconclusive check proceeds under an assumption, logged at
  `WARNING`, that versioning is off).
- `checkConditionalWriteSingleAttemptSupport` — refuses to mount writable unless the underlying
  object storage supports a single-HTTP-attempt retry profile for conditional writes. A hidden SDK
  retry can outlive the writer's mount lease and obscure whether a conditional operation actually
  committed, so retries on the conditional path must be explicit CAS state-machine transitions, not
  transparent client behavior.

A third, optional probe (`probeConditionalCopy`) checks whether the backend enforces a write-once
conditional server-side copy. It only matters for `staging_backend = s3`: when the probe reports
`false`, S3-native staging silently falls back to local staging rather than refusing to mount, since
enforcement here is an optimization, not a correctness requirement of the disk itself.
