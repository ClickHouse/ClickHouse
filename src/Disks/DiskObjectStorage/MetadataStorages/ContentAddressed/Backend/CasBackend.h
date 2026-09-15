#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasTransportAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasWriteOnceKey.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/types.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace DB::Cas
{

/// Typed erasure evidence for one key or one prefix. Ordinary `head`/`read` answer only two things
/// for a plain caller: the object, or nullopt for the store's own authoritative "key not found" (see
/// `isObjectNotFound`) -- every other fault (a missing bucket/container, a permission failure, a
/// transport fault) propagates as an exception instead of being flattened into absence. That
/// present/absent/throw shape is exactly right for a plain read, but wrong for lifecycle recovery,
/// which must never treat a fault it cannot classify as proof that data is gone. `ProbeOutcome` keeps
/// the four cases distinct: only a backend's OWN authoritative "not found" evidence earns `KeyAbsent`
/// -- a timeout, a 5xx, or an unclassifiable error is ALWAYS `Indeterminate`, never promoted to absence.
enum class ProbeOutcome : uint8_t
{
    Present,           /// the key (or, for a prefix probe, at least one object under it) exists
    KeyAbsent,         /// authoritative miss: the container is alive, the key itself is not there
    ContainerAbsent,   /// the bucket/prefix-parent itself is gone, not merely the key
    AccessDenied,      /// the probe was rejected on permissions — absence was never established
    Indeterminate,     /// a transport/timeout/unclassifiable error — absence was NEVER proven
};

/// Result of `Backend::probeSentinelRaw`. `body` carries the materialized bytes only when the outcome is
/// `Present`.
struct SentinelProbeResult
{
    ProbeOutcome outcome;
    std::optional<String> body;
};

/// Re-readable payload transport for one unconditional blob publication. `fresh_envelope` is the
/// complete CAS envelope to prepend, while `payload_size` is the exact number of bytes the source
/// must yield before the backend can make the destination visible.
struct StreamingBlobPublication
{
    uint64_t payload_size;
    String fresh_envelope;
    std::function<std::unique_ptr<ReadBuffer>()> open_payload;
};

/// A complete, already-size-verified CAS object that can be copied byte-for-byte to the destination.
/// This transport requires a provider-native same-store copy; the backend must never replace it with
/// a client-side read/write fallback.
struct VerbatimStagedBlobPublication
{
    String object_key;
    uint64_t object_size;
};

using BlobPublication = std::variant<StreamingBlobPublication, VerbatimStagedBlobPublication>;

/// Transport-only request for an unconditional blob rewrite. Publication does not inspect destination
/// state or freshness metadata and does not produce an incarnation token.
struct BlobPublishRequest
{
    String destination_key;
    BlobPublication publication;
};

namespace blob_publication_detail
{

struct BlobPayloadCopyResult
{
    uint64_t copied = 0;
    bool has_excess = false;

    bool exact(uint64_t expected) const
    {
        return copied == expected && !has_excess;
    }
};

/// Copy no more than the declared payload, then consume one byte solely to distinguish exact input
/// from a long source. The probe byte never reaches `to`, so callers can cancel without having sent
/// more than the promised body to their destination transport.
inline BlobPayloadCopyResult copyBlobPayloadBounded(ReadBuffer & from, WriteBuffer & to, uint64_t expected)
{
    uint64_t remaining = expected;
    while (remaining != 0 && !from.eof())
    {
        const size_t available = static_cast<size_t>(from.buffer().end() - from.position());
        const size_t count = static_cast<size_t>(std::min<uint64_t>(remaining, available));
        to.write(from.position(), count);
        from.position() += count;
        remaining -= count;
    }

    BlobPayloadCopyResult result{.copied = expected - remaining};
    if (remaining == 0)
    {
        char excess;
        result.has_excess = from.read(excess);
    }
    return result;
}

}

/// Etag-aware storage seam used by the content-addressed pool. INCARNATION SEMANTICS ARE THE
/// CONTRACT:
///   - every present key has exactly one current incarnation identified by an opaque backend value;
///   - `write` with an `expected_value` succeeds only against that exact current value (or expected
///     absence);
///   - `remove` removes ONLY the incarnation whose value matches — a mismatch MUST report
///     `RawRemoval::Mismatch` with the object untouched (backends that silently ignore the condition
///     are rejected by `Cas::Probe`).
///
/// VALUE ⟹ CONTENT PRECONDITION (read-path caches depend on this): a value must uniquely identify the
/// byte-content of the incarnation it labels — i.e. `head(k)`'s value equalling a prior `read(k)`'s
/// value MUST imply the bytes are unchanged. The protocol's SAFETY only needs the contrapositive
/// (changed bytes ⟹ a new value, so a stale conditional write/delete is rejected), but `Cas::Pool`'s
/// read-path decode cache (`readShardDecoded`) skips a re-`read`+decode on a value match, so a backend
/// whose value could REPEAT across different content would make it serve stale manifests (wrong
/// results). Holds for every backend in use: S3 ETag is content-derived; the emulated/in-memory
/// backends mint a strictly-monotonic sequence that is never reused. A backend with a weak/recycled
/// value must NOT be used as a Cas pool. The capability probe currently verifies conditional-operation
/// behavior but does not test value non-reuse across different contents, so this invariant remains a
/// requirement of every backend implementation.
///
/// Most ops take/return whole `String` bodies — sufficient for manifests, trees, and probe/GC
/// objects. Large content blobs use the transport-only `publish` seam; reads stay String-based because
/// blob payload reads go through the wiring's read stack, not this seam.
class Backend
{
public:
    Backend();
    virtual ~Backend() = default;

    /// ---- The transport primitives ----
    ///
    /// These are the ONLY methods that reach the store. Each takes a `TransportAccess`, which nothing
    /// outside `CasRequests` can construct, so no caller can reach the store without the request
    /// contract's retry, deadline and fence rules. They deal in the store's own strings: an
    /// incarnation VALUE means no more here than "what the store answered", and every grammar,
    /// key-binding and dialect check on it belongs to `CasRequests`.

    /// An object's bytes together with the value naming the incarnation they were read from.
    struct Raw     { String bytes; String value; };
    /// One object's size and incarnation value, without its body.
    struct RawMeta { uint64_t size; String value; };
    /// One listed key; `value` is present only on a backend that surfaces per-key incarnations
    /// through LIST -- see `supportsListTokens`.
    struct RawListedKey { String key; uint64_t size; std::optional<String> value; };
    struct RawListPage  { std::vector<RawListedKey> keys; String next_cursor; };
    /// The store refused the write's precondition. Nothing was written; what the key holds now is
    /// whatever a read finds. An expected outcome, never an error.
    struct RawConflict {};
    /// `DeleteMarker` is a removal that did NOT reclaim: a versioned bucket archived a noncurrent
    /// version instead. Distinct from `Removed` because reclaiming the storage is the point.
    enum class RawRemoval : uint8_t { Removed, Gone, Mismatch, DeleteMarker };

    /// Reads the whole object, or nullopt when the key is absent.
    virtual std::optional<Raw>     read  (const String & key, TransportAccess &) = 0;
    /// One point-in-time observation of the current incarnation's size and value; nullopt when absent.
    virtual std::optional<RawMeta> head  (const String & key, TransportAccess &) = 0;
    /// One page of keys under `prefix`, resuming strictly after `cursor`; an empty `next_cursor`
    /// marks the end of the enumeration.
    virtual RawListPage            list  (const String & prefix, const String & cursor, size_t limit, TransportAccess &) = 0;
    /// Removes ONLY the incarnation whose value equals `expected_value`; a mismatch must leave the
    /// object untouched.
    virtual RawRemoval             remove(const String & key, const String & expected_value, TransportAccess &) = 0;
    /// Creates the key (`expected_value == nullopt`) or replaces exactly the incarnation named by
    /// `expected_value`. Returns the store's own value for what it just wrote, UNVALIDATED: a value
    /// that fails the dialect grammar means the write may still have landed, which the caller settles
    /// by reading the key back, and which this seam must never report as corruption.
    virtual std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                                     const std::optional<String> & expected_value, TransportAccess &) = 0;
    /// A forward-only read of a WRITE-ONCE object (runs, seals): nothing is materialized by the seam.
    /// MUTABLE objects (root shards, gc/state, mounts) MUST use `read` -- their bytes may change
    /// under an open stream. Null when the key is absent.
    virtual std::unique_ptr<ReadBuffer> stream(const String & key, TransportAccess &) = 0;
    /// Executes one unconditional blob publication and returns once the complete destination is
    /// visible. Transport only: it observes no destination state and produces no incarnation.
    virtual void publish(const BlobPublishRequest & request, TransportAccess &) = 0;

    /// Removes up to 1000 WRITE-ONCE keys in one request with no per-key precondition; an absent key
    /// is success. The caller proves the keys are write-once by minting them as `WriteOnceKey`, so a
    /// backend never sees a mutable control key through this verb. Throws on any failure; the whole
    /// chunk is reissued by the engine, which is sound because a key already deleted is absent, and
    /// absence is success.
    virtual void removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, TransportAccess &) = 0;

    /// Authoritative, cache-bypassing probe of one key -- see `ProbeOutcome`. DEFAULT (used by every
    /// backend without sharper raw-error evidence, e.g. `InMemoryBackend`): derived from `head`/`read`
    /// alone, so it can only distinguish `Present` from `KeyAbsent`, and ANY exception from either
    /// call is `Indeterminate` -- never promoted to `KeyAbsent`. A backend able to surface real
    /// container/permission evidence (the S3-native and Local paths of `ObjectStorageBackend`)
    /// overrides this to sharpen the classification.
    virtual SentinelProbeResult probeSentinelRaw(const String & key, TransportAccess & access)
    {
        try
        {
            const auto meta = head(key, access);
            if (!meta)
                return {ProbeOutcome::KeyAbsent, std::nullopt};
            auto raw = read(key, access);
            /// Vanished between the two: still a clean, authoritative miss, not an error.
            if (!raw)
                return {ProbeOutcome::KeyAbsent, std::nullopt};
            return {ProbeOutcome::Present, std::move(raw->bytes)};
        }
        catch (...)
        {
            return {ProbeOutcome::Indeterminate, std::nullopt};
        }
    }

    /// The dialect this backend mints its incarnation values in.
    virtual Dialect dialect() const = 0;

    /// Identifies this backend INSTANCE, so an incarnation observed elsewhere can be refused rather
    /// than used as a precondition here. Assigned at construction and never reused in this process.
    uint64_t backendId() const { return backend_id; }

    /// The budget for one HTTP attempt in milliseconds; 0 when this backend has no such notion. The
    /// request contract reserves it before every attempt it starts.
    virtual uint64_t attemptTimeoutMs() const { return 0; }

    /// What one attempt may cost end to end, connect included; the contract reserves THIS. A backend
    /// with no connect notion answers its attempt timeout. A decorator that forwards `attemptTimeoutMs`
    /// to an inner backend must forward THIS too -- the default falls back to `attemptTimeoutMs`, which
    /// would silently drop the inner backend's connect contribution.
    virtual uint64_t attemptEnvelopeMs() const { return attemptTimeoutMs(); }

    /// Asks the storage to re-acquire credentials. TRUE when fresh ones were installed, so the
    /// caller's reissue can sign with them; FALSE when this backend has no refresh mechanism, which
    /// makes an expired-credential failure terminal for the caller's policy rather than retryable.
    virtual bool refreshCredentials() { return false; }

    /// Capability fact about the LIST seam: TRUE iff this backend can surface a per-key incarnation
    /// value through `list` (i.e. each `RawListedKey` carries a value that uniquely identifies the
    /// current incarnation of that key, matching what `head` would return).
    ///
    /// Why this matters: S3 ETags are content-derived and are returned in list responses; the
    /// in-memory backend mints a monotonic value it can also surface through `list`. A backend that
    /// cannot surface per-key values through `list` MUST return FALSE.
    ///
    /// FALSE ⇒ GC `discover` must read every root-shard body to learn the current incarnation (fail
    /// closed). TRUE  ⇒ `discover` may skip an unchanged root-shard body read when the listed value
    /// equals the persisted folded one, saving a GET per unchanged shard.
    virtual bool supportsListTokens() const = 0;

    /// Pool-level preconditions beyond per-op conditional semantics — checked by the capability
    /// probe BEFORE the op battery. Default: nothing to check. The S3 backend refuses here when a
    /// generation-dialect (GCS) bucket is verified to have object versioning enabled: a token-exact
    /// DELETE against a versioned bucket archives a noncurrent generation instead of reclaiming
    /// storage, so GC "reclaim" would silently stop reclaiming. A probe that cannot answer is not
    /// evidence of that, so it warns and the mount proceeds.
    virtual void checkPoolPreconditions() {}

    /// Fail-closed precondition: may this backend serve a WRITABLE mount that skips the access-check
    /// battery? `PoolConfig::skip_access_check` is a preflight convenience, so it is available only to
    /// backends whose correctness does not depend on the battery having run. Default: available.
    /// See ObjectStorageBackend's override for the one combination that refuses it.
    virtual void checkSkipAccessCheckSupport() {}

    /// Fail-closed precondition: a Native-mode backend MUST have a
    /// working single-attempt conditional-write path before it coordinates a WRITABLE pool — silently
    /// running CAS conditional writes under the disk's default (~500-attempt) transparent retry policy
    /// is exactly the hazard this seam forbids. Asked by `Pool::open` through
    /// `backendForCapabilityPredicates()`, alongside `checkPoolPreconditions`, before the probe
    /// operation is admitted. Default: nothing to check (EmulatedSingleProcess and non-S3 backends
    /// are not gated here — see ObjectStorageBackend's override for the one backend that is).
    virtual void checkConditionalWriteSingleAttemptSupport() {}

private:
    uint64_t backend_id;
};

inline Backend::Backend()
{
    /// Per instance, monotonic, never reused: an incarnation carries the id of the backend that
    /// observed it, so it can be refused anywhere else. Starts at 1, leaving 0 naming no backend.
    static std::atomic<uint64_t> next_backend_id{1};
    backend_id = next_backend_id.fetch_add(1, std::memory_order_relaxed);
}

using BackendPtr = std::shared_ptr<Backend>;

}
