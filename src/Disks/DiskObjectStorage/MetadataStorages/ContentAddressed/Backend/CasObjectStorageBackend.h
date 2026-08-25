#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <deque>
#include <map>
#include <mutex>

#include "config.h"

namespace DB::Cas
{

/// Fold and point GETs commonly read tiny bodies (about 3.7 KiB on the measured workload), while the
/// default `ReadBufferFromS3` allocation is about 1 MiB. If the caller already knows the object size,
/// use `ReadSettings::adjustBufferSize` to request a buffer of `known_size + slack`, without exceeding
/// the caller's configured default. A zero `known_size` means that the size is unknown and preserves
/// the supplied settings unchanged.
constexpr uint64_t CAS_FOLD_READ_SLACK_BYTES = 4096;
ReadSettings casSizedReadSettings(const ReadSettings & base, uint64_t known_size);

#if USE_AWS_S3
namespace detail
{
/// Finalize a conditional write (the condition rode on the buffer's WriteSettings) and map a
/// precondition loss to an OUTCOME — anything else propagates. This is the classifier for the
/// typed `S3Exception` signal; exposed here for unit tests only — production callers go through
/// `ObjectStorageBackend`. See the definition for the exact matching rules.
PutOutcome finalizeConditionalWrite(WriteBuffer & buf);
}
#endif

/// Production Backend over IObjectStorage.
///
/// Native mode (S3-like): conditions ride the existing plumbing — WriteSettings
/// object_storage_write_if_none_match / object_storage_write_if_match (consumed by WriteBufferFromS3)
/// and IObjectStorage::removeObjectIfTokenMatches. Tokens are backend ETags from getObjectMetadata.
/// Trust is NEVER assumed: Cas::Probe validates enforcement per pool at open.
///
/// EmulatedSingleProcess mode (LocalObjectStorage — tests and local development ONLY): the object
/// storage has no conditional ops, so this adapter provides EXACT token semantics itself with a
/// process-wide mutex and an in-memory per-key token MINTED FROM the object's own etag (mtime-ns on
/// LocalObjectStorage) — see emuMintToken. Every emulated token IS the object's current etag (not
/// merely "seeded" for pre-existing keys); this is what keeps token-exact semantics correct ACROSS a
/// process restart, which a plain in-process counter cannot do (codex-review-triage §3.18, 19c): a
/// counter restarts at 0 and can re-mint a value colliding with a persisted pre-restart delete token
/// for a completely different incarnation, while a resurrected body's mtime is always later. Semantics
/// otherwise hold within ONE process only — exactly what unit tests need.
class ObjectStorageBackend final : public Backend
{
public:
    /// Unhide the base convenience overloads (omitted Range/ObjectMeta/expected-token forms): the
    /// overrides below would otherwise shadow them for callers holding a concrete backend type.
    using Backend::get;
    using Backend::getStream;
    using Backend::putIfAbsent;
    using Backend::putIfAbsentStream;
    using Backend::putOverwrite;
    using Backend::casPut;

    enum class Mode { Native, EmulatedSingleProcess };

    /// Construct a backend over `object_storage`. Native mode uses the storage's conditional
    /// operations and native token dialect; `EmulatedSingleProcess` serializes operations locally for
    /// tests and local development. The generation-token store limit applies only to Native mode:
    /// generation stores must use a single PUT because their multipart completion path does not enforce
    /// the precondition.
    ObjectStorageBackend(ObjectStoragePtr object_storage_, Mode mode_, uint64_t token_producing_single_put_cap_ = 1ULL << 30);

    /// Read an object or return `nullopt` if it is absent. Native mode HEADs first so the returned
    /// token identifies the incarnation whose bytes are read; a not-found race is also reported as
    /// `nullopt`, while unrelated storage errors propagate.
    std::optional<GetResult> get(const String & key, Range range) override;
    /// Open a forward-only ranged stream for a write-once object. The stream is not materialized in
    /// memory; mutable objects must use `get` because their contents may change while it is open.
    std::optional<GetStreamResult> getStream(const String & key, Range range) override;
    /// Return the current size, attributes, and incarnation token, or an absent `HeadResult`.
    HeadResult head(const String & key) override;
    /// S3 ETags are content-derived and surfaced in list responses — TRUE for ETag-token Native
    /// and EmulatedSingleProcess modes. FALSE on a generation-token store (GCS): the XML LIST
    /// surfaces MD5-style ETags in the response BODY, which the header-level response adaptation
    /// cannot map to generations. A list-derived token would therefore be an invalid
    /// `If-Match` token; generation stores deliberately omit it and make GC re-read each shard.
    /// Consumers already treat absent list tokens as Read/fail-closed (GC discover re-reads every
    /// shard — a cost, not a correctness change).
    bool supportsListTokens() const override { return native_token_type != TokenType::Generation; }

    /// Create `key` only if it is absent. On a precondition failure the object is untouched and the
    /// result has no token; on success the token identifies the newly written incarnation.
    PutResult putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta) override;

    /// Native mode: true streaming — bytes flow straight into the object storage's write buffer with
    /// `If-None-Match: *` riding on the request. EmulatedSingleProcess mode: memory-buffered delegation
    /// to putIfAbsent (acceptable: this mode exists for unit tests only).
    WriteSinkPtr putIfAbsentStream(const String & key, const ObjectMeta & meta) override;
    /// Replace `key` only when its current token exactly equals `expected`; a mismatch leaves the
    /// existing incarnation untouched. Storage exceptions propagate instead of being reported as a
    /// successful or failed precondition.
    PutResult putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta) override;
    /// Perform a compare-and-set: `expected == nullopt` means create-if-absent. A conflict leaves the
    /// object untouched; a committed result carries the new incarnation token.
    CasResult casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta) override;
    /// Remove only the incarnation matching `token`, preserving the object on a mismatch and exposing
    /// whether the storage created a delete marker.
    DeleteOutcome deleteExact(const String & key, const Token & token) override;
    /// Return a page after `cursor`; the next cursor is the last returned key and is empty at the end.
    ListPage list(const String & prefix, const String & cursor, size_t limit) override;

    /// `promoteStaged` (S3-native staging, Native mode only — EmulatedSingleProcess has no server-side
    /// conditional copy and is never selected for S3 staging, so it throws `NOT_IMPLEMENTED` there):
    /// WRITE-ONCE conditional copy via `IObjectStorage::copyObjectConditional`.
    /// `resurrect` (every mode): prepends `fresh_header` and UNCONDITIONALLY writes
    /// `[fresh_header][payload]` to `blob_key` (a fresh tag gives a token distinct from the condemned
    /// incarnation, so its already-queued exact-token delete misses), then a fresh HEAD for the token.
    /// The write carries no precondition but IS token-producing, so it goes through
    /// `tokenProducingWriteSettings`: on a generation dialect that forces a single PUT and applies the
    /// token-producing cap, exactly as a conditional write does; an ETag dialect is unconstrained.
    /// EmulatedSingleProcess materializes and SERIALIZES resurrections process-wide, bounding the peak
    /// to one body.
    PutResult promoteStaged(const String & staging_key, const String & blob_key) override;
    Token resurrect(ReadBuffer & payload, uint64_t payload_size, const String & blob_key, const String & fresh_header) override;

    /// Pool-level precondition: on a Native, generation-dialect (GCS) backend, reject the pool unless
    /// object versioning is VERIFIABLY disabled — see Backend::checkPoolPreconditions.
    void checkPoolPreconditions() override;

    /// Fail-closed precondition: a Native, generation-dialect (GCS) backend refuses a writable mount
    /// that asked to skip the access check — see Backend::checkSkipAccessCheckSupport.
    void checkSkipAccessCheckSupport() override;

    /// Fail-closed precondition for writable Native mode: require that the object storage supports the
    /// SingleAttempt retry profile (ObjectStorageRetryProfile), which disables transparent
    /// conditional-write retries. Without it, an SDK retry could cross the mount lease boundary or turn
    /// an uncertain result into a misleading precondition failure. A non-S3 object storage used for
    /// test construction reports no support; this check is the mount-time gate. No-op for
    /// `EmulatedSingleProcess`.
    void checkConditionalWriteSingleAttemptSupport() override;

    /// See Backend::probeSentinelRaw. Native: a raw HEAD via `IObjectStorage::getObjectMetadata` (the
    /// THROWING variant — unlike `tryGetObjectMetadata`/`nativeHead`, it never swallows the S3 error),
    /// classified by S3 error code. EmulatedSingleProcess (Local): stats the configured container
    /// directory (`emu_root`) first — `ContainerAbsent` if it is gone — then the key.
    SentinelProbeResult probeSentinelRaw(const String & key) override;

    /// The token kind this backend's object storage mints: TokenType::ETag for AWS-compatible
    /// stores, TokenType::Generation when the storage mints GCS generations (the
    /// generation rides the ETag plumbing; the VALUE stays opaque either way).
    TokenType nativeTokenType() const { return native_token_type; }
    void setNativeTokenTypeForTest(TokenType t) { native_token_type = t; }

    /// ---- Token policy (single source of truth; see the .cpp) ----
    /// A GCS generation reaches this layer through the AWS SDK's ETag field, which the HTTP boundary
    /// fills with an ETag-shaped — that is, quoted — value. A generation is a number, and quotes are
    /// transport syntax that must not enter CAS protocol state, where token values are compared for
    /// equality and written into persisted manifests. Strip them here, where the meaning changes from
    /// "an ETag field" to "an incarnation token".
    ///
    /// Generation-scoped on purpose: an ETag-dialect token IS the quoted ETag, and those quotes are
    /// required syntax when the value goes back out as `If-Match`. Stripping unconditionally would
    /// corrupt the AWS-compatible path.
    String normalizeTokenValue(const String & etag) const
    {
        if (native_token_type != TokenType::Generation)
            return etag;
        if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"')
            return etag.substr(1, etag.size() - 2);
        return etag;
    }

    /// Mint the incarnation token for a key we just HEAD'd or wrote: the object ETag/generation
    /// string carried under this backend's native dialect (native_token_type).
    ///
    /// This is the ONLY site that mints a Generation token: `tokenForList` is the sole other
    /// `native_token_type` mint, and `supportsListTokens` above returns false for Generation, so it
    /// cannot produce one.
    Token tokenForHead(const String & etag) const
    {
        return Token{normalizeTokenValue(etag), native_token_type};
    }

    /// The token to surface for a LISTED key: present iff this backend surfaces per-key list tokens
    /// (supportsListTokens — FALSE on a generation store, where a list-derived token is a poisoned
    /// If-Match) AND the listing carried a non-empty etag. Matches what tokenForHead would return.
    std::optional<Token> tokenForList(const String & etag) const
    {
        if (!supportsListTokens() || etag.empty())
            return std::nullopt;
        return Token{etag, native_token_type};
    }

    /// Whether an observed incarnation token satisfies an expected one: exact identity (value AND
    /// type). Every conditional compare in this backend goes through here.
    static bool tokenMatches(const Token & observed, const Token & expected)
    {
        return observed == expected;
    }

    /// Settings for EVERY write whose result token enters CAS protocol state ("Write-settings
    /// decomposition"): always NativeConditional request mode, plus -- on a generation-token store
    /// (GCS) only -- a forced single PUT capped at token_producing_single_put_cap (GCS enforces no
    /// precondition on CompleteMultipartUpload, so any token-producing write, conditional or not,
    /// would silently overwrite instead of failing if it took the multipart path). Use this directly
    /// for an UNCONDITIONAL token-producing write (resurrection); conditionalWriteSettings layers a
    /// precondition-specific retry policy on top of it for compare/create operations.
    WriteSettings tokenProducingWriteSettings() const;
    WriteSettings tokenProducingWriteSettingsForTest() const { return tokenProducingWriteSettings(); }
    /// Settings for a Native COMPARE/CREATE write (create-if-absent, compare-and-set): everything
    /// tokenProducingWriteSettings sets, plus exactly one attempt at every retry layer (the
    /// SingleAttempt object-storage retry profile and WriteBufferFromS3's own unexpected-error retry
    /// loop) and skipping the racy post-upload existence/size check.
    WriteSettings conditionalWriteSettings() const;
    WriteSettings conditionalWriteSettingsForTest() const { return conditionalWriteSettings(); }
    /// Convert a successful write/copy response's incarnation-identifying string into this backend's
    /// token -- the ONE place that decides how strictly to trust it ("Exact successful-write token").
    /// Generation dialect (GCS): the response MUST carry a non-empty, purely numeric generation; a
    /// missing or non-numeric value is an exception -- there is no follow-up HEAD, so a broken or
    /// lying response can never be silently patched over by a later, unrelated read. Every other
    /// dialect (ETag, and any backend with no write-time token at all, e.g. local files) keeps the
    /// pre-existing behavior: an absent value falls back to a fresh HEAD of `key`.
    Token tokenFromWriteResult(const String & key, const std::optional<String> & etag);
    /// Override the emulated backend's wall clock for deterministic expiry tests.
    void setEmuNowNsForTest(uint64_t now_ns);
    /// Return the guarded per-key token-state size for expiry tests.
    size_t emuTokenStateSizeForTest() const;

private:
    const ObjectStoragePtr object_storage;
    const Mode mode;
    TokenType native_token_type = TokenType::ETag;
    /// GCS single-PUT budget for every token-producing write (generation-token stores only --
    /// unconditional writes, such as resurrection, included; see tokenProducingWriteSettings and ctor).
    const uint64_t token_producing_single_put_cap;

    /// EmulatedSingleProcess state: per-key {etag, disambiguator} — see emuMintToken. A successfully
    /// deleted entry is retained only while its etag is recent enough that an immediate recreate could
    /// land in the same mtime quantum. `deleteExact` erases already-old entries immediately and queues
    /// recent ones for the bounded lazy sweep in emuMintToken, so a key need not be revisited to expire.
    /// The queue records the exact state generation deleted; a subsequent re-mint makes the record
    /// obsolete rather than allowing it to erase the live incarnation's token state.
    mutable std::mutex emu_mutex;
    std::map<String, std::pair<String, uint64_t>> emu_token_state;
    struct EmuTokenExpiry
    {
        uint64_t queued_at_ns;
        String key;
        std::pair<String, uint64_t> token_state;
    };
    std::deque<EmuTokenExpiry> emu_token_expiry;
    uint64_t emu_now_ns_for_test = 0;
    /// Fallback nonce for the (anomalous) case where the object storage reports an EMPTY etag: mints a
    /// fresh, unpersisted value each time — never worse than the old counter for that case, but never
    /// masquerading as a real etag-derived identity either.
    uint64_t emu_seq = 0;

    /// Look up Native metadata and convert the storage ETag or generation to this backend's token. On
    /// a generation-token store, the minted token is validated exactly like a write result (see
    /// isValidGenerationTokenValue) before this returns it: a missing/malformed x-goog-generation on an
    /// otherwise-successful HEAD would otherwise mint an invalid token here with no check at all, one
    /// layer before tokenFromWriteResult's own check on the write path.
    std::optional<HeadResult> nativeHead(const String & key);

    /// True iff `value` is a well-formed generation: non-empty and every character an ASCII digit.
    /// Shared by nativeHead and tokenFromWriteResult so the two places that mint a Generation token
    /// from a remote response cannot drift apart on what "valid" means. Deliberately NOT folded into
    /// tokenForHead, which stays a pure minter with no opinion on the value it is handed.
    static bool isValidGenerationTokenValue(const String & value);
    /// Write a body with the condition already encoded in `ws`, finalize it, classify a lost
    /// precondition, and return the new token when the write succeeds.
    PutResult nativeConditionalPut(const String & key, const String & bytes, const WriteSettings & ws, const ObjectMeta & meta);

    /// §3.18 №19 hardening: whether `t` is the dialect this backend itself mints (native_token_type
    /// for Native mode, always TokenType::Emulated for EmulatedSingleProcess). Every conditional
    /// mutation checks this BEFORE touching the wire (Native forwards only Token::value as the
    /// If-Match/removeObjectIfTokenMatches argument, blind to Token::type) or comparing values
    /// (Emulated) — a foreign-dialect token is rejected locally rather than trusted to the remote
    /// backend, or to a value-space that was never designed to discriminate it.
    bool mintingTypeMatches(TokenType t) const { return t == (mode == Mode::Native ? native_token_type : TokenType::Emulated); }

    /// ---- Emulated helpers (caller holds emu_mutex) ----
    ///
    /// EmulatedSingleProcess resolves logical keys under the object storage's common key prefix (its
    /// root), so each backend instance is physically isolated — a real object store likewise scopes keys
    /// to a bucket/prefix. The token map is keyed by the LOGICAL key (prefix-independent).
    const String emu_root;             /// object_storage->getCommonKeyPrefix() captured at construction
    String emuPath(const String & key) const;   /// logical key -> physical object-storage path

    /// The caller holds `emu_mutex` for all five helpers below, preserving the exists/read and
    /// observe/write checks as one process-local operation.
    bool emuExists(const String & key) const;
    String emuRead(const String & key, Range range) const;
    /// Write a body as the new incarnation of `key` and return its freshly minted token (the
    /// object's own post-write etag — see emuMintToken).
    Token emuWrite(const String & key, const String & bytes, const ObjectMeta & meta);
    /// Return the current emulated token for a key we just read/HEAD'd, reflecting its on-disk etag —
    /// does NOT advance the same-etag disambiguator (that only applies to a just-completed write).
    Token emuObserveToken(const String & key);
    uint64_t emuNowNs() const;
    /// Examine a fixed number of oldest deleted-state records, expiring only an exact current match.
    void emuPruneTokenState(uint64_t now_ns);
    /// Single source of truth for minting an emulated token from an observed `etag`: the wire value IS
    /// the etag while it is the first thing minted for `key` at that etag, or `etag#N` once a SAME-etag
    /// rewrite forces a disambiguator (`just_wrote` — see the mtime-quantum note in emu_token_state's
    /// declaration and codex-review-triage §3.18 19c step 4). An empty `etag` (the storage could not
    /// report one) falls back to a fresh, UNPERSISTED monotonic value from emu_seq.
    Token emuMintToken(const String & key, const String & etag, bool just_wrote);
};

}
