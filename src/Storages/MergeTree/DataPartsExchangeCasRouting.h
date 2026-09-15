#pragma once

#include <Core/Types_fwd.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using Disks = std::vector<DiskPtr>;
}

namespace Poco
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}
using LoggerPtr = Poco::LoggerPtr;

namespace DB::DataPartsExchange
{

/// The receiver's content-addressed pool advertise as it goes on the wire (the `cas_pool_uuid` request
/// parameter): the pool ids of every disk of its storage policy that could take a relink — sorted,
/// deduplicated, joined with ", ". The list form and the ", " delimiter are the ones the zero-copy
/// `remote_fs_metadata` capability list already uses, so the exchange keeps one list convention (the
/// decoder differs in one respect: an empty string is no pool at all, never one empty id). A single id
/// is written verbatim: a receiver with one pool puts on the wire exactly the string that a sender
/// comparing the whole value with its own pool id matches. Empty ids are dropped (a storage that never
/// started has no pool id and nothing to advertise).
String encodeCasPoolAdvertise(Strings pool_uuids);
Strings decodeCasPoolAdvertise(const String & text);

/// Which pool a relink offer is for. The sender names it in the `cas_pool_uuid` response cookie, and
/// the answer is that cookie ONLY if it is one of the pools this receiver advertised — the advertise is
/// the receiver's question, and a byte re-request after a failed relink advertises nothing, so a peer
/// offering regardless can never select a disk. A sender that predates the cookie can only have matched
/// a one-element advertise, so an absent cookie means that single pool. Several advertised pools and no
/// cookie is not a state an honest sender can produce, and the answer is "no pool" — the receiver never
/// guesses.
String resolveOfferedCasPool(const Strings & advertised_pools, const String & offered_pool_cookie);

/// One content-addressed disk of the RECEIVING table's storage policy, in policy order.
struct CasRelinkCandidate
{
    String disk_name;
    String pool_uuid;        /// empty: the storage never started; never a candidate
    bool read_only = false;  /// a static property of the disk's configuration; the one exclusion
};

/// Which candidate receives the offered relink: the index of the first candidate on the offered pool
/// (`resolveOfferedCasPool`) that is not read-only. `nullopt` means no disk of this policy may take
/// the offer, which the caller turns into a byte fetch. Whether the pool is LIVE is deliberately not
/// part of this decision — a not-live pool disk is still the target, the relink's own write gate
/// refuses it, and the fetch fails and is retried rather than landing on another disk.
std::optional<size_t> resolveForcedCaCandidate(
    const std::vector<CasRelinkCandidate> & candidates,
    const Strings & advertised_pools,
    const String & offered_pool_cookie);

/// The outcome of resolving a relink offer against this receiver's candidates: the pool the offer is
/// for (needed even when no disk is forced, to check a caller-supplied disk against it later), and the
/// disk to force the fetch onto — null when the caller already supplied a disk, or no live-policy
/// candidate matches the offered pool.
struct ForcedCaDiskChoice
{
    String offered_pool;
    DiskPtr disk;
};

/// Resolve a relink offer's pool, and — only when the caller left disk selection to the fetch itself —
/// pick the forced candidate (`resolveForcedCaCandidate`) to place it on, ahead of the storage policy's
/// own placement. `part_name` and `log` are for the log lines only; the `cas_relink_receiver_drop_forced_disk`
/// failpoint (test-only) lives here so it can stand in for an offer this policy has no disk for.
ForcedCaDiskChoice chooseForcedCaDisk(
    bool caller_supplied_disk,
    const std::vector<CasRelinkCandidate> & candidates,
    const Disks & candidate_disks,
    const Strings & advertised_pools,
    const String & offered_pool_cookie,
    const String & part_name,
    LoggerPtr log);

/// One content-addressed disk of the SENDING table's storage policy, as the confirm routing sees it.
struct CasConfirmRoutingCandidate
{
    const void * exchange_identity = nullptr;  /// the `IContentAddressedExchange` behind the disk
    String pool_uuid;
    bool owns_namespace = false;
};

/// Which candidate answers a relink confirm for `pool_uuid`: EXACTLY one distinct mount that owns the
/// namespace, else `nullopt` — zero owners, or two distinct mounts, are both ambiguous and `Unknown`
/// is the only honest answer. Disks that alias one mount (a base disk and its cache wrapper share the
/// exchange object) count once, as the first of them.
std::optional<size_t> resolveConfirmRoutingCandidate(
    const std::vector<CasConfirmRoutingCandidate> & candidates,
    const String & pool_uuid);

}
