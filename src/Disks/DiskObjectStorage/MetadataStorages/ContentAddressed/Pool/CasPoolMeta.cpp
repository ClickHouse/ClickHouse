#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_STATE;
}
}

namespace DB::Cas
{

namespace
{

/// Two `thread_local_rng` u64 draws composed into a 128-bit id.
UInt128 mintPoolId()
{
    const UInt128 hi = thread_local_rng();
    const UInt128 lo = thread_local_rng();
    return (hi << 64) | lo;
}

/// Whether `config_algo` is already a registered member of `pool.algos_used`. Membership, not the
/// opt-in flag, is the steady-state check. `algos_used` is kept sorted, so this is a binary search.
bool isAlgoAdmittedIn(const PoolMeta & pool, BlobHashAlgo config_algo)
{
    const auto v = static_cast<uint8_t>(config_algo);
    return std::binary_search(pool.algos_used.begin(), pool.algos_used.end(), v);
}

/// Renders `algos_used` as "ch128, sha256" for the refusal message below.
String joinAlgoNames(const std::vector<uint8_t> & algos_used)
{
    String out;
    for (size_t i = 0; i < algos_used.size(); ++i)
    {
        if (i != 0)
            out += ", ";
        out += blobHashAlgoName(static_cast<BlobHashAlgo>(algos_used[i]));
    }
    return out;
}

/// Fail-closed on a non-admitted algo without the opt-in flag: admission is EXPLICIT
/// opt-in; the default stays fail-closed -- a changed config alone must never silently turn a pool
/// mixed. Never touches the pool.
[[noreturn]] void throwNotAdmitted(const PoolMeta & pool, BlobHashAlgo config_algo)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "CAS pool blob_hash mismatch: pool has {{{}}}; config requests {}; set "
        "<blob_hash_allow_new>1</blob_hash_allow_new> to admit a new algo into this pool",
        joinAlgoNames(pool.algos_used), blobHashAlgoName(config_algo));
}

/// The relaxed admission check (replaces an earlier fail-close that required the single pool algo to match):
/// `pm`/`token` are the most-recently-read `_pool_meta` state (present, decoded, valid). Already a
/// member of `algos_used` => OK, no write (steady state). Not a member and `!allow_new` =>
/// `BAD_ARGUMENTS` (the pool is never touched). Not a member and `allow_new` => CAS-union `config_algo`
/// into `algos_used` (recomputed from the FRESH value on every retry -- union-only, so there is no
/// ABA) and raises `min_reader_generation` to THIS build's own floor (`G_BUILD`, `CasFormat.h`) in
/// the SAME write (first registration of a schema-3-bearing algo also raises
/// `min_reader_generation` -- a build that cannot decode schema-3 settlement state has an OLDER
/// `G_BUILD` and is correctly refused by the startup gate once a future generation bump lands here).
/// On a CAS conflict, re-read and retry the whole decision (a concurrent admitter may have unioned a
/// DIFFERENT algo, or the very one we wanted, in the meantime).
PoolMeta admitOrValidate(
    Backend & backend, const String & key, PoolMeta pm, Token token,
    BlobHashAlgo config_algo, bool allow_new)
{
    for (;;)
    {
        if (isAlgoAdmittedIn(pm, config_algo))
            return pm;

        if (!allow_new)
            throwNotAdmitted(pm, config_algo);

        PoolMeta next = pm;
        next.algos_used.push_back(static_cast<uint8_t>(config_algo));
        std::sort(next.algos_used.begin(), next.algos_used.end());
        next.min_reader_generation = G_BUILD;

        const CasResult res = backend.casPut(key, encodePoolMeta(next), token);
        if (res.outcome == CasOutcome::Committed)
            return next;

        auto fresh = backend.get(key);
        if (!fresh)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS pool meta: '{}' vanished mid-admission (conflicting write then a concurrent delete)", key);
        pm = decodePoolMeta(fresh->bytes);
        token = fresh->token;
        /// loop: re-evaluate membership against the FRESH pm (never re-encode the stale `next`)
    }
}

}

PoolMeta PoolMeta::createOrValidate(
    Backend & backend, const Layout & layout, uint64_t blob_header_len, uint64_t gc_shards,
    BlobHashAlgo blob_hash_algo, bool allow_new, bool allow_mint)
{
    /// The passed config is the caller's responsibility — reject bad values before any I/O.
    validatePoolBlobHeaderLen(blob_header_len, ErrorCodes::BAD_ARGUMENTS, "pool meta");
    if (gc_shards == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "CAS pool meta: gc_shards must be >= 1");
    /// Defense against a garbage `static_cast` past the caller's own boundary: `blobHashAlgoName`
    /// throws BAD_ARGUMENTS for anything `BlobHashAlgo` does not actually admit.
    blobHashAlgoName(blob_hash_algo);

    const String key = layout.poolMetaKey();

    /// Present => the pool is authoritative; ignore the passed config's blob_header_len and run the
    /// flag-gated admission check rather than the old single-value fail-close.
    if (auto existing = backend.get(key))
    {
        PoolMeta pm = decodePoolMeta(existing->bytes);
        return admitOrValidate(backend, key, std::move(pm), existing->token, blob_hash_algo, allow_new);
    }

    /// Absent => mint a pool id and try to create the object with `algos_used = {blob_hash_algo}`.
    /// Every pool this build creates is schema-3-shaped from birth (schemas 1/2 do not exist in this
    /// build at all), so the reader-generation floor is stamped at THIS build's `G_BUILD` at
    /// creation, not left at 0.
    ///
    /// BOOTSTRAP GATE (spec §2 [C4][D2]): minting is permitted ONLY on the verified bootstrap path. A
    /// non-bootstrap caller (a read-only/observe open, `openForDecommission`) passes `allow_mint=false`
    /// and fails closed here — never minting a fresh identity outside that path (an observe scan that
    /// minted would poison the next writable mount's residual check). The residual EMPTINESS proof itself
    /// cannot live here: it must precede ANY write (spec's "zero-write residual check FIRST, before ANY
    /// probe write"), and by the time `createOrValidate` runs the capability battery has already written
    /// to the prefix, so `Pool::open` runs `probePoolBootstrapResidual` up front and only then passes
    /// `allow_mint=true`. `pool_prefix` is exclusively CAS-owned.
    if (!allow_mint)
        throw Exception(ErrorCodes::INVALID_STATE,
            "CAS pool meta: _pool_meta absent — refusing to mint outside the verified bootstrap path; "
            "run a writable mount (or recreate the pool)");

    PoolMeta pm;
    pm.pool_id = mintPoolId();
    pm.blob_header_len = blob_header_len;
    pm.gc_shards = gc_shards;
    pm.min_reader_generation = G_BUILD;
    pm.algos_used = {static_cast<uint8_t>(blob_hash_algo)};

    if (backend.casPut(key, encodePoolMeta(pm), /*expected*/ std::nullopt).outcome == CasOutcome::Committed)
        return pm;

    /// Lost the race: the winner's object MUST be present now. The loser UNIONS its algo via the SAME
    /// flag-gated admission path as a reopen, instead of the old unconditional fail-close.
    auto winner = backend.get(key);
    if (!winner)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS pool meta: create-if-absent reported Conflict but '{}' is absent on re-read", key);
    PoolMeta winner_pm = decodePoolMeta(winner->bytes);
    return admitOrValidate(backend, key, std::move(winner_pm), winner->token, blob_hash_algo, allow_new);
}

}
