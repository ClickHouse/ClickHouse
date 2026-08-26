---
description: 'Fetch by relink between two replicas sharing a pool: the gates in order, what actually seals commit-before-release, and detach/attach/drop.'
sidebar_label: 'Replication'
sidebar_position: 7
slug: /antalya/cas/architecture/replication
title: 'CAS Architecture — Replication'
doc_type: 'reference'
---

# CAS architecture — replication {#replication}

When two `ReplicatedMergeTree` replicas share a `CAS` pool, a fetch should move **no bytes** — the
receiver already has access to the same blobs the sender does. The mechanism is a three-phase
handshake, fetch by relink, layered directly on the ordinary interserver part-fetch protocol. This
page covers the handshake, the gates that decide whether it fires, what actually makes it safe
against a concurrent `GC` round, and how detach/attach/drop reduce to the same primitives. The
writer owns table semantics and part publication (see the
[part-lifecycle page](/antalya/cas/architecture/part-lifecycle)); `GC` owns ref-log folding and
physical cleanup (see the [garbage-collection page](/antalya/cas/architecture/garbage-collection))
— ordinary replication traffic never reads `gc/state` or waits on a `GC` round.

## The handshake {#handshake}

Only two of the three phases are round trips to the sender — the offer and the confirm. The
publish and the promote are the receiver's own writes to the pool.

```mermaid
sequenceDiagram
    autonumber
    participant R as Receiver
    participant Snd as Sender
    participant S3 as Shared pool

    R->>Snd: GET part, cas_pool_uuid = R's pool uuid, client_protocol_version = 11
    Note over R: advertising 11 is a promise to confirm before promoting
    Snd->>Snd: same disk pool uuid? identity, never endpoint plus prefix
    Snd->>S3: resolve the offer once -- manifest bytes and confirm token from the SAME view
    Snd-->>R: cookie cas_relink = part_manifest_v2, cookie cas_source_token = ..., body = manifest bytes
    Note over Snd: sender is fire-and-forget -- it releases the part here

    rect rgba(120,160,255,0.12)
    Note over R,S3: T1 -- publish, the plus-one lands first
    R->>S3: adopt entries by evidence, no HEAD, no bytes, stageManifest fresh receiver-local id, precommitAdd
    Note over R: the sender's ManifestRef, namespace and digest are ignored -- only entries are used
    end

    rect rgba(255,190,120,0.15)
    Note over R,Snd: T2 -- confirm
    R->>Snd: POST cas_confirm = token
    Snd->>Snd: confirmExactRef, zero object-store I/O, never throws
    Snd-->>R: cookie cas_confirm_answer = yes or unproven
    end

    alt answer is yes
        R->>S3: T3 -- promote, ref published
    else anything else -- unproven, missing cookie, timeout, transport error
        R->>R: throw a locally generated NETWORK_ERROR, retry later
        Note over R: never a byte re-request -- that would go back to the very source whose state is in doubt
    end
```

## The gates, in order {#relink-gates}

| # | Gate | What it enforces |
|---|---|---|
| 1 | Pool identity | The receiver advertises `cas_pool_uuid`; the sender offers relink only if its own disk's pool uuid is **equal**. Matching by endpoint and prefix was tried and rejected — a minted pool uuid is the identity |
| 2 | Protocol version 11 | On the receiver side, advertising it is a promise to run the confirm round trip before promoting |
| 3 | One resolution for two outputs | The manifest bytes and the confirm token come from the **same** view. Two separate calls would allow a repoint in between and hand the receiver a token naming a manifest whose entries it never adopted |
| 4 | The receiver trusts nothing from the wire but the entry list | The sender's manifest id, namespace and payload digest are ignored; the target namespace and ref come from the receiver's own router, and manifest path hygiene is validated at decode |
| 5 | The confirm is I/O-free and fail-closed | A cold, evicted, unfenced or terminal mount answers `Unknown`. `No` and `Unknown` both go on the wire as `unproven`, because the fence check is evaluated last, so a `No` cannot be distinguished from "cannot prove it right now" |
| 6 | Only the literal `yes` authorizes promotion | Everything else — including a timeout — is one outcome: throw and retry later |
| 7 | Promote outcomes are three-way | `Committed` proceeds; a **proven** not-committed state (body-absent precommit, precommit no longer live owner, ref conflict) falls back to a byte fetch; `Unresolved` **throws**, because returning "fall back" there would publish the part twice |

The byte-fetch fallback is bounded: it re-invokes the fetch with relink disabled, which stops the
receiver advertising its pool uuid, which stops the sender offering relink — so the relink path
cannot be entered twice for one fetch. Byte-fetched files content-address and dedup on arrival
anyway, so falling back never loses the dedup property, only the zero-byte-move property for that
one fetch.

## What actually seals "commit before release" {#relink-seal}

The receiver's `+1` — its precommit binding — is durable **before** the sender is asked anything,
and any removal of the sender's own binding is appended strictly after that `+1` is in the ref
log. That ordering, steps T1 then T2 then T3, is the whole seal.

This does **not** establish that every subsequent `GC` fold *sees* that `+1` under every listing
behavior: a configuration with one incomplete listing page can, in principle, let a fold miss a
freshly published edge. A confirmed relink therefore proves only "the source still holds exactly
this manifest right now", not "no future fold can ever miss this edge" — `ca-fsck`'s
reachable-but-absent scan is the backstop for that gap, not the relink protocol itself. Relink
also races `GC` in the ordinary sense any writer does: between the sender encoding its offer and
the receiver's promote, `GC` on the shared pool may condemn a blob that was live only through the
sender's own ref. The [writer-versus-GC race](/antalya/cas/architecture/blob-protocol#writer-gc-race)
on the blob-protocol page is what makes that interleaving safe — revival is re-upload only, and the
receiver's evidence-adopt is protected by its own durable precommit edge exactly like any other
writer's adopt.

A fetch whose source part is still a live, held `DataPartPtr` on the sender's own replica — the
common case for a local, same-process relink — keeps the source pinned through the destination's
commit by ordinary part-lifetime rules, independent of the ref-log seal above.

## Detach, attach, drop {#detach-attach-drop}

A detached part is **not** a separate namespace — it is a ref in the table's own namespace with a
`detached/` prefix (the same is true of `moving/`). Only `FREEZE` uses a separate shadow namespace,
but it remains under the root that created it: `<server_root_id>/shadow/<backup>/…`. The ownership
check therefore attributes it to exactly that server root, under the same strict prefix rule as live
content, and that root can confirm its exact refs.

`DETACH`, `ATTACH`, `delete_tmp_` cleanup, and merge-result renames all reduce to the same two
moves: re-key any *staged* source into the destination, then `republishRef(src → dst)` for any
*committed* source. `republishRef` re-reads the source manifest freshly, publishes an
equivalent-entry manifest under the destination ref — a **new** manifest id, with blobs untouched
and adopted by evidence — then drops the source ref. A destination that already exists with
identical entries just drops the source, an idempotent re-drive; one with different entries
throws.

Manifests are therefore per-ref and never moved: a detach creates a new manifest for
`detached/<part>` and retires the old one, and the blobs' net in-degree is unchanged.
