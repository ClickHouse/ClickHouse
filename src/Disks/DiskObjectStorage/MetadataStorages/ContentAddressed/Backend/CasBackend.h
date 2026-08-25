#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

/// User metadata carried alongside an object (S3 x-amz-meta-*). The CA store uses exactly one entry,
/// "cas_owner" = "<server_id_hex>:<epoch>:<build_seq>" — the owner triple the GC watermark reads.
using ObjectMeta = std::map<String, String>;

/// A byte window requested from an object. An absent length means that the window extends to EOF.
/// Backends use the same semantics for materialized and forward-only reads: the offset is exact,
/// while a backend may expose an advisory end when its underlying read buffer cannot enforce one.
struct Range
{
    uint64_t offset = 0;
    std::optional<uint64_t> length;   /// nullopt => to the end
    bool whole() const { return offset == 0 && !length; }
};

/// Materialized object bytes together with the incarnation and user metadata observed by the read.
/// The token identifies the exact object version whose bytes are in `bytes`; callers may use it to
/// validate a subsequent token-conditional mutation.
struct GetResult
{
    String bytes;
    Token token;       /// token of the incarnation the bytes came from
    ObjectMeta attributes;
};

/// A forward-only read of a WRITE-ONCE object (runs, seals): nothing is materialized by the seam.
/// MUTABLE objects (root shards, gc/state, mounts) MUST keep using `get` — their bytes may change
/// under an open stream. `token` identifies the incarnation the stream reads, same as `get`.
struct GetStreamResult
{
    std::unique_ptr<ReadBuffer> stream;
    Token token;
};

/// Metadata returned by `Backend::head`. For an absent key, `exists` is false and the other fields
/// retain their defaults; for a present key, `size`, `token`, and `attributes` describe one current
/// incarnation as observed by the backend.
struct HeadResult
{
    bool exists = false;
    uint64_t size = 0;
    Token token;
    ObjectMeta attributes;
};

/// Outcome of a write-once create or a token-conditional overwrite. A precondition failure means
/// that the backend preserved the existing object; it is an expected result, not an exception.
enum class PutOutcome : uint8_t
{
    Done,                 /// object written; the returned PutResult.token is the new incarnation's token
    PreconditionFailed,   /// If-None-Match hit an existing key / If-Match mismatched — nothing changed
};

/// Outcome of a compare-and-set write. `Conflict` means that the expected token (or expected
/// absence) did not match and that the backend left the object unchanged.
enum class CasOutcome : uint8_t
{
    Committed,
    Conflict,             /// expected token (or absence) did not match — nothing changed
};

/// Result of a backend write: the outcome plus the resulting object token (previously a `Token * out_token`
/// out-parameter). `token` is set ONLY when the write actually landed an incarnation (a `Done`/`Committed`
/// outcome); on `PreconditionFailed`/`Conflict` nothing was written and `token` is left default-constructed,
/// exactly mirroring the old contract where callers only read `*out_token` on success.
template <typename Outcome>
struct WriteResultT
{
    Outcome outcome;
    Token token;
};

using PutResult = WriteResultT<PutOutcome>;
using CasResult = WriteResultT<CasOutcome>;

/// Result of deleting one exact incarnation. `TokenMismatch` and `NotFound` are deliberately
/// distinct: the former proves that another incarnation is now current, while the latter means
/// there is no object to remove. `created_delete_marker` exposes a storage-versioning behavior
/// that is incompatible with current-object reclamation.
struct DeleteOutcome
{
    enum class Kind : uint8_t { Deleted, TokenMismatch, NotFound } kind = Kind::NotFound;
    /// TRUE if the backend reported a delete marker was created because versioning is enabled. The
    /// capability probe rejects this for the current-object storage model: exact deletion must reclaim
    /// the current object rather than archive a noncurrent version.
    bool created_delete_marker = false;
};

/// A key returned by `Backend::list`. The `token` field is populated ONLY when the backend
/// returns TRUE from `supportsListTokens` — it identifies the key's current incarnation, matching
/// what `head` would return for the same key at that instant. Callers that do not need the token
/// (e.g. GC fence sweep, orphan sweep) ignore the field; GC discover uses it to skip unchanged
/// root shards.
struct ListedKey
{
    String key;
    uint64_t size = 0;
    std::optional<Token> token;   /// present iff supportsListTokens() == true
};
/// One page returned by `Backend::list`. `keys` contains only the requested prefix and the cursor
/// resumes strictly after the last returned key; an empty cursor marks the end of the enumeration.
struct ListPage
{
    std::vector<ListedKey> keys;
    String next_cursor;       /// Last returned key; empty => no more pages.
};

/// Typed erasure evidence for one key or one prefix. `head`/
/// `get` deliberately flatten every kind of miss (a clean absence, a missing bucket/container, a
/// permission failure, a transport fault) into one "not found" result, which is exactly right for
/// their callers (a plain read) but wrong for lifecycle recovery, which must never treat a
/// transport/permission failure as proof that data is gone. `ProbeOutcome` keeps the four cases
/// distinct: only a backend's OWN authoritative "not found" evidence earns `KeyAbsent` — a timeout,
/// a 5xx, or an unclassifiable error is ALWAYS `Indeterminate`, never promoted to absence.
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

/// Streaming conditional create (If-None-Match:* semantics). The caller writes the FULL object body
/// (envelope header + payload) into buffer, then calls finalize exactly once:
///   - Done                ⇒ the object is durable; the returned PutResult.token is the new incarnation's token
///   - PreconditionFailed  ⇒ the key already existed — NOTHING was changed (same contract as putIfAbsent)
/// finalize may throw on storage errors; PreconditionFailed is an OUTCOME, never an exception.
/// cancel (or destruction before finalize) abandons the upload: the key is never created by it.
///
/// MISUSE/LIFETIME CONTRACT: after finalize or cancel the sink is DEAD — any further finalize,
/// cancel, or write into buffer is a programming error (finalize asserts on it in debug builds).
/// The caller must not call the underlying buffer's own finalize/cancel directly — only through
/// the sink. A sink is single-caller: it is NOT thread-safe (only Backend itself is), and it must
/// not outlive the Backend that created it.
class WriteSink
{
public:
    virtual ~WriteSink() = default;
    virtual WriteBuffer & buffer() = 0;
    virtual PutResult finalize() = 0;
    virtual void cancel() noexcept = 0;
};

using WriteSinkPtr = std::unique_ptr<WriteSink>;

/// Token-aware storage seam used by the content-addressed pool. TOKEN SEMANTICS ARE THE CONTRACT:
///   - every present key has exactly one current incarnation identified by an opaque Token;
///   - putOverwrite/casPut succeed only against the expected current token (or expected absence);
///   - deleteExact removes ONLY the incarnation whose token matches — wrong token MUST be a
///     TokenMismatch with the object untouched (backends that silently ignore the condition are
///     rejected by `Cas::Probe`);
///   - conditional PUTs are protocol hygiene; casPut and deleteExact are SAFETY-critical.
///
/// TOKEN ⟹ CONTENT PRECONDITION (read-path caches depend on this): a token must uniquely identify
/// the byte-content of the incarnation it labels — i.e. `head(k).token == prior get(k).token` MUST
/// imply the bytes are unchanged. The protocol's SAFETY only needs the contrapositive (changed
/// bytes ⟹ a new token, so a stale CAS/delete is rejected), but `Cas::Pool`'s read-path decode
/// cache (`readShardDecoded`) skips a re-`get`+decode on a token match, so a backend whose token
/// could REPEAT across different content would make it serve stale manifests (wrong results). Holds
/// for every backend in use: S3 ETag is content-derived; the emulated/in-memory backends mint a
/// strictly-monotonic sequence that is never reused. A backend with a weak/recycled token must NOT
/// be used as a Cas pool. The capability probe currently verifies conditional-operation behavior but
/// does not test token non-reuse across different contents, so this invariant remains a requirement
/// of every backend implementation.
///
/// Most ops take/return whole `String` bodies — sufficient for manifests, trees, and probe/GC
/// objects. LARGE content blobs stream through `putIfAbsentStream` (see `WriteSink`); reads stay
/// String-based because blob payload reads go through the wiring's read stack, not this seam.
class Backend
{
public:
    virtual ~Backend() = default;

    /// Reads the selected bytes and their token, or returns nullopt when the key is absent. For a
    /// mutable object, callers must use this materialized form so the body is fixed before parsing.
    virtual std::optional<GetResult> get(const String & key, Range range) = 0;
    std::optional<GetResult> get(const String & key) { return get(key, {}); }

    /// Forward-only stream over the object's `range` (default: whole object) for WRITE-ONCE objects
    /// (runs, seals). The returned `stream` yields exactly the window's bytes and nothing is
    /// materialized whole by the seam — the caller reads at its own pace. MUTABLE objects (root
    /// shards, gc/state, mounts) MUST keep using `get`: their bytes can change under an open stream.
    /// CAVEAT: the window END is advisory on storages where `setReadUntilPosition` is a hint
    /// (LocalObjectStorage) — the stream may yield bytes past the window; consumers MUST bound their
    /// own consumption (RunFileReader bounds to its data_end). The window START is always exact.
    virtual std::optional<GetStreamResult> getStream(const String & key, Range range) = 0;
    std::optional<GetStreamResult> getStream(const String & key) { return getStream(key, {}); }

    /// Returns the current incarnation's existence, size, token, and metadata without reading its
    /// body. The result describes one point-in-time observation; a later operation must use the
    /// returned token when it needs to protect against replacement.
    virtual HeadResult head(const String & key) = 0;

    /// Creates `key` only when it is absent. `PreconditionFailed` leaves the existing object intact;
    /// storage failures are reported as exceptions. On success, the returned token identifies the
    /// newly created incarnation.
    virtual PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) = 0;
    PutResult putIfAbsent(const String & key, const String & bytes) { return putIfAbsent(key, bytes, {}); }
    /// Streaming variant of putIfAbsent — see WriteSink. Large content blobs use this; whole-String
    /// ops remain for manifests, trees, probe and GC objects.
    virtual WriteSinkPtr putIfAbsentStream(const String & key, const ObjectMeta & meta) = 0;
    WriteSinkPtr putIfAbsentStream(const String & key) { return putIfAbsentStream(key, {}); }

    /// Replaces the current object only when its token equals `expected`. A mismatch leaves the
    /// object unchanged and returns `PreconditionFailed`; the returned token is meaningful only on
    /// `Done`.
    virtual PutResult putOverwrite(const String & key, const String & bytes, const Token & expected,
                                   const ObjectMeta & meta) = 0;
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected)
    {
        return putOverwrite(key, bytes, expected, {});
    }
    /// expected == nullopt => create-if-absent CAS (the first write of a root manifest).
    /// A non-null expected token conditionally replaces that exact current incarnation. Conflicts
    /// leave the object unchanged and are returned as an outcome rather than an exception.
    virtual CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected,
                             const ObjectMeta & meta) = 0;
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected)
    {
        return casPut(key, bytes, expected, {});
    }

    /// Deletes only the current incarnation identified by `token`. A token mismatch must leave the
    /// object untouched; the result distinguishes that case from an already absent key.
    virtual DeleteOutcome deleteExact(const String & key, const Token & token) = 0;

    /// Lists one page of keys under `prefix`, starting after `cursor` and returning at most `limit`
    /// entries. `ListPage::next_cursor` is the only supported continuation state.
    virtual ListPage list(const String & prefix, const String & cursor, size_t limit) = 0;

    /// Capability fact about the LIST seam: TRUE iff this backend can surface a per-key incarnation
    /// token through `list` (i.e. each `ListedKey` carries a token that uniquely identifies the
    /// current incarnation of that key, matching what `head` would return).
    ///
    /// Why this matters: S3 ETags are content-derived and are returned in list responses; the
    /// in-memory backend mints a monotonic token it can also surface through `list`. A backend that
    /// cannot surface per-key tokens through `list` MUST return FALSE.
    ///
    /// FALSE ⇒ GC `discover` must read every root-shard body to learn the current token (fail closed).
    /// TRUE  ⇒ `discover` may skip an unchanged root-shard body read when the listed token equals
    ///          the persisted folded token, saving a GET per unchanged shard.
    virtual bool supportsListTokens() const = 0;

    /// Pool-level preconditions beyond per-op conditional semantics — checked by the capability
    /// probe BEFORE the op battery. Default: nothing to check. The S3 backend fails closed here
    /// unless a generation-dialect (GCS) bucket is VERIFIABLY free of object versioning: a
    /// token-exact DELETE against a versioned bucket archives a noncurrent generation instead of
    /// reclaiming storage, so GC "reclaim" would silently stop reclaiming.
    virtual void checkPoolPreconditions() {}

    /// Fail-closed precondition: may this backend serve a WRITABLE mount that skips the access-check
    /// battery? `PoolConfig::skip_access_check` is a preflight convenience, so it is available only to
    /// backends whose correctness does not depend on the battery having run. Default: available.
    /// See ObjectStorageBackend's override for the one combination that refuses it.
    virtual void checkSkipAccessCheckSupport() {}

    /// Fail-closed precondition: a Native-mode backend MUST have a
    /// working single-attempt conditional-write path before it coordinates a WRITABLE pool — silently
    /// running CAS conditional writes under the disk's default (~500-attempt) transparent retry policy
    /// is exactly the hazard this seam forbids. Checked by the capability probe alongside
    /// checkPoolPreconditions. Default: nothing to check (EmulatedSingleProcess and non-S3 backends
    /// are not gated here — see ObjectStorageBackend's override for the one backend that is).
    virtual void checkConditionalWriteSingleAttemptSupport() {}

    /// Authoritative, cache-bypassing probe of one key — see `ProbeOutcome`. DEFAULT (used by every
    /// backend without sharper raw-error evidence, e.g. `InMemoryBackend`): derived from `head`/`get`
    /// alone, so it can only distinguish `Present` from `KeyAbsent`, and ANY exception from either
    /// call is `Indeterminate` — never promoted to `KeyAbsent`. A backend able to surface real
    /// container/permission evidence (the S3-native and Local paths of `ObjectStorageBackend`)
    /// overrides this to sharpen the classification.
    virtual SentinelProbeResult probeSentinelRaw(const String & key)
    {
        try
        {
            const HeadResult hr = head(key);
            if (!hr.exists)
                return {ProbeOutcome::KeyAbsent, std::nullopt};
            auto g = get(key);
            /// Vanished between head and get: still a clean, authoritative miss, not an error.
            if (!g)
                return {ProbeOutcome::KeyAbsent, std::nullopt};
            return {ProbeOutcome::Present, std::move(g->bytes)};
        }
        catch (...)
        {
            return {ProbeOutcome::Indeterminate, std::nullopt};
        }
    }

    /// WRITE-ONCE conditional SERVER-SIDE COPY of `staging_key` to `blob_key` (`If-None-Match:*` on the
    /// destination) — the S3-native staging promote's create primitive. `Done` + `token` = the
    /// destination ETag (the new incarnation token, exactly the role the
    /// streaming `putIfAbsentStream` PUT's ETag plays) when this call created `blob_key`;
    /// `PreconditionFailed` when `blob_key` already existed — NOTHING was changed (same write-once
    /// contract as `putIfAbsentStream`). No LIVE object is ever overwritten by this call.
    ///
    /// DEFAULT: fail closed (`NOT_IMPLEMENTED`) — a backend without a native, enforced conditional
    /// server-side copy is NEVER selected for S3 staging (the mount-time probe fell back to Local
    /// staging + `putIfAbsentStream`), so this must throw rather than silently degrade to an
    /// unconditional overwrite. The caller must use local staging when this primitive is unavailable.
    virtual PutResult promoteStaged(const String & /*staging_key*/, const String & /*blob_key*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cas::Backend::promoteStaged (write-once server-side copy) is not implemented for this backend");
    }

    /// UNCONDITIONAL re-upload of the writer's OWN payload over `blob_key` under a FRESH-tagged
    /// envelope header — the sanctioned condemned-object resurrection overwrite. Writes
    /// `[fresh_header][payload]`, streaming `payload` from `reader`, and returns the fresh incarnation's
    /// token. A Native backend streams the payload and never materializes it whole; on an ETag-dialect
    /// store that means no size cap, while a generation-token store (GCS) enforces the same single-PUT
    /// token-producing cap this write would face if it carried a precondition (GCS drops preconditions
    /// on multipart completion regardless of whether one was set). The emulated backend materializes
    /// (its conditional ops are whole-`String` by design) and serializes resurrections to bound the
    /// peak to one body at a time.
    ///
    /// The reader is the caller's: it is ALWAYS the writer's own source (a staging object or a local
    /// staged file), NEVER a read of the condemned `blob_key`, and the caller has already skipped any
    /// envelope header on it. `fresh_header` must carry a freshly-minted `incarnation_tag`, which is what
    /// makes the resurrected body — and hence its ETag/token — differ from the condemned incarnation, so
    /// a queued exact-token delete of that incarnation can never match the live resurrection
    /// (`INV-NO-RETURN`).
    ///
    /// UNCONDITIONAL is deliberate. An `If-Match` on the condemned token would save a redundant
    /// re-upload when another writer resurrects the same blob first, and would prevent nothing: two
    /// racing resurrections write payload-identical bodies, no consumer reads a dep token's VALUE, and
    /// durable references name content hashes rather than incarnations.
    ///
    /// The caller MUST have observed the current incarnation as `Condemned` (per-hash meta point-read)
    /// before calling this. That observation is NOT re-checked at the write: two racing resurrections
    /// of the same blob may both run, and the loser overwrites the winner's FRESH incarnation. That is
    /// accepted, not prevented -- the payloads are content-identical by construction and durable
    /// references name content hashes, so the overwrite rotates the envelope and token of an
    /// equivalent body. What must never be overwritten is a live incarnation of DIFFERENT content,
    /// and that is guaranteed by the content address itself, not by this call.
    /// DEFAULT: fail closed (`NOT_IMPLEMENTED`), same rationale as `promoteStaged`.
    /// `payload_size` is the payload byte count the caller verified at staging time. The write COUNTS
    /// while streaming and MUST abort -- publishing nothing -- when the reader yields a different
    /// number of bytes. With an unconditional write this is the last line of defence: a source
    /// truncated after hashing would otherwise displace the condemned incarnation with a short body
    /// that the content address does not match, and a post-write check can only detect it AFTER the
    /// malformed incarnation became current (and can even inspect a racing writer's incarnation
    /// instead of its own).
    virtual Token resurrect(ReadBuffer & /*payload*/, uint64_t /*payload_size*/, const String & /*blob_key*/,
                            const String & /*fresh_header*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cas::Backend::resurrect is not implemented for this backend");
    }
};

using BackendPtr = std::shared_ptr<Backend>;

/// Walk every key under `prefix` exactly once, resuming by the backend's explicit last-returned-key
/// cursor (`ListPage::next_cursor`, empty => done). This centralizes the pagination contract shared by
/// GC, fsck, and cleanup sweeps: each returned key is delivered once, and the backend's cursor is the
/// only state used to request the next page.
///
/// `on_page_fetched`, if set, fires exactly once per physical `backend.list` call (including an
/// empty/undersized final page) — a GC-owned caller's hook for a page-level ProfileEvents counter,
/// without misattributing a non-GC caller (e.g. fsck) that leaves it unset. Trails `page_limit`
/// (rather than sitting before it) so the two existing callers that override `page_limit`
/// (`Gc::fold`, `CasFsck.cpp`'s `listAll`) can override `page_limit` without changing callback order.
inline void forEachListedKey(Backend & backend, const String & prefix,
                             const std::function<void(const ListedKey &)> & cb,
                             size_t page_limit = 1000,
                             const std::function<void()> & on_page_fetched = {})
{
    String cursor;
    for (;;)
    {
        const ListPage page = backend.list(prefix, cursor, page_limit);
        if (on_page_fetched)
            on_page_fetched();
        for (const ListedKey & k : page.keys)
            cb(k);
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
}

/// The normalized verdict of a token-exact delete, unifying the DeleteOutcome::Kind three-way that GC
/// (blob + manifest delete) and the orphan-manifest sweep each mapped by hand.
enum class DeleteClass : uint8_t { Deleted, Absent, Replaced };

/// Converts a backend-specific delete outcome into the three states used by cleanup callers. The
/// default branch is fail-safe: an unknown value is treated as `Replaced`, so cleanup never reports
/// an unverified deletion as successful.
inline DeleteClass classifyDeleteOutcome(const DeleteOutcome & d)
{
    switch (d.kind)
    {
        case DeleteOutcome::Kind::Deleted:       return DeleteClass::Deleted;
        case DeleteOutcome::Kind::NotFound:      return DeleteClass::Absent;
        case DeleteOutcome::Kind::TokenMismatch: return DeleteClass::Replaced;
    }
    return DeleteClass::Replaced;   /// unreachable; fail-safe toward "leave it" (never a false Deleted)
}

/// Returns the stable lowercase label used when reporting a normalized delete result. Unknown enum
/// values are labeled `replaced`, matching `classifyDeleteOutcome`'s fail-safe behavior.
inline std::string_view deleteClassName(DeleteClass c)
{
    switch (c)
    {
        case DeleteClass::Deleted:  return "deleted";
        case DeleteClass::Absent:   return "absent";
        case DeleteClass::Replaced: return "replaced";
    }
    return "replaced";
}

}
