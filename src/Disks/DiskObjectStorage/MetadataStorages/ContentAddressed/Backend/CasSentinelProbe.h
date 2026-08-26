#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>

namespace DB::Cas
{

/// Authoritative, cache-bypassing probe of one key. NEVER conflates transport errors with absence:
/// timeouts / 5xx / connection errors => Indeterminate; permission errors => AccessDenied;
/// missing container/bucket/prefix-parent => ContainerAbsent; a clean authoritative miss => KeyAbsent.
///
/// Free-function entry point (spec §2) — a thin dispatch to the backend's own typed-evidence
/// classification (`Backend::probeSentinelRaw`; see there for the per-backend semantics: the
/// S3-native raw HEAD error, the Local container-directory stat, or the generic head/get-based
/// default for a backend without sharper evidence).
SentinelProbeResult probeSentinel(Backend & backend, const String & key);

/// Verdict of the zero-write startup bootstrap residual check ("Startup ordered vs the capability
/// probe"). Before a writable `Pool::open` runs
/// the MUTATING `_probe/` capability battery, it must decide whether it is safe to bootstrap a missing
/// `_pool_meta`. `pool_prefix` is EXCLUSIVELY CAS-owned: a fresh pool identity may be minted ONLY over a
/// genuinely empty prefix — never over residual data an incomplete erase left behind (that would strand
/// the old objects as orphans under a new, colliding identity, the "restart poisons a partially-erased
/// pool" hole this check closes).
enum class BootstrapResidual : uint8_t
{
    /// `<prefix>/_pool_meta` exists → the pool is authoritative; proceed with the normal open/validate.
    PoolMetaPresent,
    /// No `_pool_meta`, and every listed object is structurally-valid `_probe/` debris (or the prefix is
    /// empty) → safe to bootstrap a fresh pool.
    EmptyOrProbeOnly,
    /// No `_pool_meta`, and the only durable object is a byte-for-byte canonical empty
    /// `cas/ref_catalog` (plus structurally-valid `_probe/` debris). This is the sole retryable
    /// pre-meta bootstrap residue: a prior opener made the mandatory catalog durable but did not
    /// publish `_pool_meta`.
    CanonicalEmptyCatalogOnly,
    /// No `_pool_meta`, but at least one non-`_probe` object exists → refuse to bootstrap (typed startup
    /// failure, zero writes performed).
    ResidualWithoutMeta,
    /// The authoritative LIST itself failed → emptiness could not be proven → fail-closed (never mint a
    /// fresh identity while residual data cannot be ruled out).
    Indeterminate,
};

/// Zero-write authoritative classification of a pool prefix for the startup bootstrap decision. A single
/// paginated LIST of `layout.poolPrefix()`; each listed object is classified as the `_pool_meta`
/// sentinel, capability-battery debris under the reserved `<prefix>/_probe/` subtree (a crash-mid-battery
/// leftover OR a concurrent fresh opener's in-flight battery — [D2]), or genuine residual CAS state. It
/// NEVER writes, and it IGNORES probe debris exactly so a normal restart after a crash-mid-battery still
/// bootstraps cleanly. On non-strong-LIST backends this is the single best-effort authoritative check the
/// weaker guarantee allows — still fail-closed on any residual object found. Used by `Pool::open` BEFORE
/// the capability battery so that no probe write ever precedes the emptiness proof.
BootstrapResidual probePoolBootstrapResidual(Backend & backend, const Layout & layout);

}
