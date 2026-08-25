#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestControl.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <Core/Defines.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>

#include <Common/Exception.h>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

#include <algorithm>
#include <chrono>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

ObjectStorageBackend::ObjectStorageBackend(ObjectStoragePtr object_storage_, Mode mode_, uint64_t conditional_single_put_cap_)
    : object_storage(std::move(object_storage_))
    , mode(mode_)
    , conditional_single_put_cap(conditional_single_put_cap_)
    , emu_root(object_storage->getCommonKeyPrefix())
{
    if (mode == Mode::Native && object_storage->conditionalOpsUseGenerationTokens())
        native_token_type = TokenType::Generation;
}

/// See Backend::checkPoolPreconditions. Only the Native, generation-dialect (GCS) combination has
/// anything to check: a token-exact DELETE on a versioned bucket archives a noncurrent generation
/// instead of reclaiming storage, so GC "reclaim" would silently stop reclaiming. Both an enabled
/// bucket and an unverifiable probe refuse the mount.
void ObjectStorageBackend::checkPoolPreconditions()
{
    if (mode != Mode::Native || native_token_type != TokenType::Generation)
        return;

    const auto versioned = object_storage->isBucketVersioningEnabled();
    if (!versioned.has_value())
    {
        /// An unverifiable probe fails the mount, exactly like a confirmed Enabled below. Proceeding
        /// on the ASSUMPTION that versioning is off was the earlier behaviour and it is not
        /// defensible: what GC does on a versioned bucket is delete objects it believes it reclaimed,
        /// so the assumption is silently wrong in precisely the case that matters, and it is wrong
        /// without bound (a warning at mount does not stop the next round). The operator can prove
        /// the bucket's state with one call and grant the permission the probe needs.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "CAS on GCS: could not VERIFY the bucket-versioning precondition (the versioning check "
            "request failed — e.g. the credential lacks permission to read it — or this backend "
            "cannot answer it) — refusing to mount writable. CAS cannot assume versioning is off: if "
            "it is actually enabled, token-exact DELETEs archive noncurrent generations instead of "
            "reclaiming storage and GC silently stops reclaiming space. Grant the credential "
            "permission to read the bucket's versioning configuration, confirm versioning is "
            "disabled, and retry the mount.");
    }

    if (*versioned)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "CAS on GCS: the bucket has object VERSIONING enabled. A token-exact DELETE on a "
            "versioned bucket archives a noncurrent generation instead of reclaiming storage — GC "
            "would silently stop reclaiming space. Disable versioning on the bucket (and prefer "
            "soft-delete duration 0 for CAS pools) and retry the mount.");
}

/// See Backend::checkSkipAccessCheckSupport. A writable generation-dialect (GCS) mount is the one
/// combination whose correctness depends on the MUTATING capability battery having run: the battery
/// is what proves a numeric generation actually reaches GCS as x-goog-if-generation-match on a
/// DELETE, and nothing else in the mount path proves it.
void ObjectStorageBackend::checkSkipAccessCheckSupport()
{
    if (mode != Mode::Native || native_token_type != TokenType::Generation)
        return;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "CAS on GCS: skip_access_check=true is not supported on a WRITABLE generation-token mount. "
        "The capability battery this setting skips is what verifies that a token-exact DELETE "
        "actually honours the generation precondition; without it GC could delete an incarnation it "
        "did not condemn, and the bucket-versioning precondition would go unchecked too. Remove "
        "skip_access_check from this disk, or mount it read-only.");
}

/// See Backend::checkConditionalWriteSingleAttemptSupport. This is a MOUNT-TIME gate, deliberately
/// separate from the ctor: narrow, targeted unit tests can keep constructing a raw Native-mode backend
/// over a non-S3 IObjectStorage (LocalObjectStorage) to exercise OTHER behaviors in isolation — the
/// established convention throughout this test suite (see e.g. gtest_cas_backend_generation.cpp). A
/// REAL writable mount, by contrast, always reaches this check: runCapabilityProbe (CasProbe.cpp) calls
/// it for every non-read-only Pool::open, so production never silently runs Native-mode conditional
/// writes under the disk's default (~500-attempt) transparent retry policy.
void ObjectStorageBackend::checkConditionalWriteSingleAttemptSupport()
{
    if (mode != Mode::Native)
        return;

    /// The property checked is now backend CAPABILITY, not client presence: whether this object
    /// storage can honor the SingleAttempt retry profile at all (S3ObjectStorage always can; a non-S3
    /// object storage like LocalObjectStorage cannot).
    const bool single_attempt_supported = object_storage->supportsRetryProfile(ObjectStorageRetryProfile::SingleAttempt);
    if (!single_attempt_supported)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "CAS Native-mode conditional writes require an object storage that supports the "
            "SingleAttempt retry profile (RFC cas-s3-timeout-retry-control), but this one does not "
            "(IObjectStorage::supportsRetryProfile returned false, or this build has no AWS S3 "
            "support) — refusing to mount writable. Native mode is designed for an S3-like "
            "conditional dialect only; a non-S3 object storage should use EmulatedSingleProcess.");
}

/// =========================================================================================
/// Native helpers
/// =========================================================================================

bool ObjectStorageBackend::isValidGenerationTokenValue(const String & value)
{
    return !value.empty() && std::all_of(value.begin(), value.end(), [](char c) { return c >= '0' && c <= '9'; });
}

std::optional<HeadResult> ObjectStorageBackend::nativeHead(const String & key)
{
    auto metadata = object_storage->tryGetObjectMetadataWithNativeToken(key, /*with_tags=*/false);
    if (!metadata)
        return std::nullopt;

    HeadResult hr;
    hr.exists = true;
    hr.size = metadata->size_bytes;
    hr.token = tokenForHead(metadata->etag);
    /// A generation-token store guarantees a numeric x-goog-generation on every successful HEAD;
    /// a missing or non-numeric value (a proxy dropping the header, a service regression) means the
    /// ordinary ETag fell through unmapped. There is no follow-up HEAD to patch this over, so surface
    /// the failure here rather than minting a token that would poison the first conditional operation
    /// that trusts it -- exactly the contract tokenFromWriteResult already enforces on the write path.
    if (native_token_type == TokenType::Generation && !isValidGenerationTokenValue(hr.token.value))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS on GCS: a HEAD of {} succeeded but its response carried no valid generation ({})",
            key, metadata->etag);
    hr.attributes = ObjectMeta(metadata->attributes.begin(), metadata->attributes.end());
    return hr;
}

/// Finalize a conditional write (the condition rode on the buffer's WriteSettings) and map a
/// precondition loss to an OUTCOME — anything else propagates.
///
/// A backend reports a lost condition as an `S3Exception` carrying the canonical S3 error code string
/// from the response XML `<Code>` (`S3Exception::getExceptionName`); a conditional-write 412 is
/// UNMODELED for the AWS SDK (its enum value is UNKNOWN), so `S3Exception::isPreconditionFailed` is the
/// typed signal — the `PreconditionFailed` name, or that token in the raw body for S3-compatible stores
/// (RustFS) whose non-AWS body the SDK cannot parse into a name. A `404 NoSuchKey` on an `If-Match` PUT
/// (the key was deleted out from under us) is treated identically: protocol callers handle 'mismatch'
/// and 'gone' the same way (re-validate), so both collapse onto `PreconditionFailed`. `NoSuchKey` IS
/// modeled by the SDK, and `WriteBufferFromS3` retries it internally surfacing the exhaustion with the
/// typed enum code (and no name), so the enum is matched as well as the name. The mapping is fail-safe in
/// direction: a misread error becomes a retryable PreconditionFailed/Conflict, never a false success.
///
/// Native conditional writes require an S3-compatible integration environment for end-to-end
/// coverage. Unit tests cover the emulated semantics, the typed exception path, and this classifier
/// through the test-only `detail` declaration.
#if USE_AWS_S3
PutOutcome detail::finalizeConditionalWrite(WriteBuffer & buf)
{
    try
    {
        buf.finalize();
    }
    catch (const S3Exception & e)
    {
        if (e.isPreconditionFailed()
            || e.getExceptionName() == "NoSuchKey"
            || e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
            return PutOutcome::PreconditionFailed;
        throw;
    }
    return PutOutcome::Done;
}
#endif

/// Build-dispatching shim for the write paths below: without the AWS SDK there is no S3Exception
/// to classify, so the errors of finalize simply propagate.
static PutOutcome finalizeConditionalWrite(WriteBuffer & buf)
{
#if USE_AWS_S3
    return detail::finalizeConditionalWrite(buf);
#else
    buf.finalize();
    return PutOutcome::Done;
#endif
}

/// Instrument the same single `finalize` call used by both Native write paths without changing their
/// `Done`/`PreconditionFailed`-or-rethrow contract. A classified precondition loss is `Unresolved`,
/// not `Committed` or a definite exception, because the response does not prove who created or
/// replaced the object; the higher-level request controller may then resolve it with exact-key state.
static PutOutcome finalizeConditionalWriteInstrumented(WriteBuffer & buf)
{
    recordConditionalWriteAttemptStarted();
    try
    {
        const PutOutcome legacy = finalizeConditionalWrite(buf);
        recordConditionalWriteOutcome(
            legacy == PutOutcome::Done ? classifyConditionalWriteResult() : CasWriteOutcome::Unresolved);
        return legacy;
    }
    catch (const std::exception & e)
    {
        recordConditionalWriteOutcome(classifyConditionalWriteResult(e));
        throw;
    }
}

/// Issue a conditional PUT (the condition rides on `ws`) and map a precondition loss — see
/// finalizeConditionalWrite. The condition is checked by the backend when the object is completed,
/// so the precondition loss always surfaces from the buffer's finalize, never from write.
PutResult ObjectStorageBackend::nativeConditionalPut(const String & key, const String & bytes, const WriteSettings & ws, const ObjectMeta & meta)
{
    std::optional<ObjectAttributes> attrs;
    if (!meta.empty())
        attrs.emplace(meta.begin(), meta.end());   /// ObjectMeta is the same map type as ObjectAttributes
    auto buf = object_storage->writeObject(
        StoredObject(key), WriteMode::Rewrite, attrs, DBMS_DEFAULT_BUFFER_SIZE, ws);
    buf->write(bytes.data(), bytes.size());
    if (finalizeConditionalWriteInstrumented(*buf) == PutOutcome::PreconditionFailed)
        return {PutOutcome::PreconditionFailed, {}};

    /// Attribute the token of the incarnation WE just wrote (model WCreate) -- see
    /// tokenFromWriteResult for the exact generation-vs-ETag policy. The S3 write returns its object
    /// ETag/generation in the PutObject/CompleteMultipartUpload response, so no follow-up HEAD is
    /// needed for most backends — this is ~73% of the CA backend's HEADs.
    return {PutOutcome::Done, tokenFromWriteResult(key, buf->getResultObjectETag())};
}

namespace
{

/// Keep the emulated backend's publication memory bound to one materialized body at a time.
std::mutex & emulatedBlobPublicationMutex()
{
    static std::mutex mutex;
    return mutex;
}

}

/// True when an exception from `IObjectStorage::readObject` means "the object is simply not there".
/// Two surfaces:
///   1. S3/RustFS:        `S3Exception` with `S3Errors::NO_SUCH_KEY` (the modeled enum — the primary
///      signal) or `getExceptionName() == "NoSuchKey"` (the canonical XML `<Code>` string, present
///      when the SDK was able to parse it; mirrors `finalizeConditionalWrite`'s detection).
///   2. Local / emulated: `DB::Exception` with `ErrorCodes::FILE_DOESNT_EXIST` (from
///      `ReadBufferFromFile` when `open(2)` returns ENOENT).
///
/// Any other error (network, auth, throttle, corruption) propagates unchanged — fail-closed.
static bool isObjectNotFound(const std::exception & e)
{
#if USE_AWS_S3
    if (const auto * s3e = dynamic_cast<const S3Exception *>(&e))
        return s3e->getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY
            || s3e->getExceptionName() == "NoSuchKey";
#endif
    if (const auto * dbe = dynamic_cast<const Exception *>(&e))
        return dbe->code() == ErrorCodes::FILE_DOESNT_EXIST;
    return false;
}

/// Read `range` of the object at `path` as a TRUE ranged read: seek to the offset and bound the
/// read window. Seek the storage buffer to the requested offset and bound the returned bytes instead
/// of reading a whole snapshot run and slicing it afterward; snapshot runs can be gigabytes at scale,
/// while the caller's memory budget is O(block).
static String readObjectRanged(IObjectStorage & object_storage, const String & path, Range range,
                               uint64_t known_size = 0)
{
    auto buf = object_storage.readObject(
        StoredObject(path), casSizedReadSettings(getReadSettings(), known_size), /*read_hint=*/std::nullopt);
    String content;
    if (range.whole())
    {
        readStringUntilEOF(content, *buf);
        return content;
    }

    /// An offset at or past EOF yields an empty result, matching the range contract of the previous
    /// whole-read implementation.
    /// `seek` past the object size may throw depending on the storage, so fail-close the window
    /// against the known size before touching the buffer position.
    /// Native callers already HEAD the key, so passing its size avoids another metadata round trip.
    /// A zero size means the caller does not know it and metadata must be fetched here.
    const uint64_t object_size = known_size != 0 ? known_size
        : object_storage.getObjectMetadata(path, /*with_tags=*/false).size_bytes;
    if (range.offset >= object_size)
        return {};

    /// The readable window, clamped to EOF. `setReadUntilPosition` is only a hint (not every object
    /// storage honors it — LocalObjectStorage does not), so the exact byte count below is what bounds
    /// the read; the hint lets storages that DO honor it avoid over-fetching.
    const uint64_t available = object_size - range.offset;
    const uint64_t to_read = range.length.has_value() ? std::min(*range.length, available) : available;

    if (range.length.has_value())
        buf->setReadUntilPosition(range.offset + *range.length);
    buf->seek(static_cast<off_t>(range.offset), SEEK_SET);

    content.resize(to_read);
    const size_t got = buf->read(content.data(), to_read);
    content.resize(got);
    return content;
}

/// Open a forward-only stream over `range` of the object at `path`, positioned at the window's first
/// byte and bounded to its last. Mirrors
/// `readObjectRanged`'s seek + bound, but RETURNS the buffer instead of draining it — the caller reads
/// at its own pace, so nothing is materialized whole. Returns nullptr when the offset is at or past EOF
/// (the empty-window clamp), matching the ranged-get contract.
static std::unique_ptr<ReadBuffer> openObjectRangedStream(IObjectStorage & object_storage, const String & path, Range range,
                                                          uint64_t known_size = 0)
{
    auto buf = object_storage.readObject(
        StoredObject(path), casSizedReadSettings(getReadSettings(), known_size), /*read_hint=*/std::nullopt);
    if (range.whole())
        return buf;

    /// Clamp exactly like `readObjectRanged`: an offset at or past EOF yields an empty stream, and
    /// `seek` past the object size may throw depending on the storage, so fail-close against the known
    /// size before touching the buffer position.
    /// As in `readObjectRanged`, a caller-supplied size avoids another metadata round trip; zero means
    /// that the size is unknown and must be fetched.
    const uint64_t object_size = known_size != 0 ? known_size
        : object_storage.getObjectMetadata(path, /*with_tags=*/false).size_bytes;
    if (range.offset >= object_size)
        return std::make_unique<ReadBufferFromString>(std::string_view{});

    /// `setReadUntilPosition` is only a hint (LocalObjectStorage does not honor it), but for a returned
    /// stream it is the only bound available — the caller drains to EOF, so a storage that DOES honor
    /// the hint stops at the window end, and one that does not over-reads only the trailing bytes.
    if (range.length.has_value())
        buf->setReadUntilPosition(range.offset + *range.length);
    buf->seek(static_cast<off_t>(range.offset), SEEK_SET);
    return buf;
}

ReadSettings casSizedReadSettings(const ReadSettings & base, uint64_t known_size)
{
    if (known_size == 0)
        return base;
    return base.adjustBufferSize(known_size + CAS_FOLD_READ_SLACK_BYTES);
}

/// =========================================================================================
/// Emulated helpers (caller holds emu_mutex)
/// =========================================================================================

namespace
{

/// The mtime-quantum guard (emuMintToken) only needs a key's `emu_token_state` entry while a
/// same-quantum tie is still POSSIBLE for a FRESH recreate — i.e. while the just-deleted
/// incarnation's own etag (mtime-ns, see emuMintToken) is recent. Once it is comfortably behind
/// "now", no later recreate can land in the same mtime quantum, so retaining the entry serves no
/// purpose (codex-review-triage §3.18, Important #1). 2 seconds is far above any filesystem's mtime
/// tick coarseness while still bounding the map to the recently-deleted-key population.
constexpr uint64_t EMU_TOKEN_STALE_AGE_NS = 2'000'000'000ULL;
constexpr size_t EMU_TOKEN_EXPIRY_SWEEP_SIZE = 16;

/// True iff `etag` parses as a plain nanosecond count (emuMintToken's `.first` is always the BARE
/// etag, never the `etag#N` disambiguated form) that is at least EMU_TOKEN_STALE_AGE_NS behind now.
/// An etag that fails to parse (e.g. a test double's non-numeric stub) is conservatively treated as
/// NOT stale — never erasing is always safe, merely un-bounded, so an unparseable value must not be
/// mistaken for a recent one.
bool etagComfortablyInThePast(const String & etag, uint64_t now_ns)
{
    if (etag.empty() || !std::all_of(etag.begin(), etag.end(), [](char c) { return c >= '0' && c <= '9'; }))
        return false;

    uint64_t etag_ns = 0;
    try
    {
        etag_ns = std::stoull(etag);
    }
    catch (...)
    {
        return false;
    }

    return now_ns > etag_ns && (now_ns - etag_ns) >= EMU_TOKEN_STALE_AGE_NS;
}

}

String ObjectStorageBackend::emuPath(const String & key) const
{
    if (emu_root.empty())
        return key;
    if (!emu_root.empty() && emu_root.back() == '/')
        return emu_root + key;
    return emu_root + "/" + key;
}

uint64_t ObjectStorageBackend::emuNowNs() const
{
    if (emu_now_ns_for_test != 0)
        return emu_now_ns_for_test;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

void ObjectStorageBackend::setEmuNowNsForTest(uint64_t now_ns)
{
    std::lock_guard lock(emu_mutex);
    emu_now_ns_for_test = now_ns;
}

size_t ObjectStorageBackend::emuTokenStateSizeForTest() const
{
    std::lock_guard lock(emu_mutex);
    return emu_token_state.size();
}

void ObjectStorageBackend::emuPruneTokenState(uint64_t now_ns)
{
    for (size_t checked = 0; checked < EMU_TOKEN_EXPIRY_SWEEP_SIZE && !emu_token_expiry.empty(); ++checked)
    {
        const auto & candidate = emu_token_expiry.front();
        auto current = emu_token_state.find(candidate.key);

        /// A later mint (including a delete+recreate in the same mtime quantum) supersedes this exact
        /// deleted state. Its queue record can be discarded immediately without touching the map.
        if (current == emu_token_state.end() || current->second != candidate.token_state)
        {
            emu_token_expiry.pop_front();
            continue;
        }

        /// Deletion time is monotonic within this mutex-protected FIFO. If its oldest record has not
        /// crossed the safety window, every later matching record is too recent as well.
        if (now_ns <= candidate.queued_at_ns || now_ns - candidate.queued_at_ns < EMU_TOKEN_STALE_AGE_NS)
            break;

        /// The record has aged enough to inspect its etag. Unparseable or otherwise uncertain etags
        /// stay in the map (fail safe), but their queue records cannot block pruning of later keys.
        if (etagComfortablyInThePast(current->second.first, now_ns))
            emu_token_state.erase(current);
        emu_token_expiry.pop_front();
    }
}

bool ObjectStorageBackend::emuExists(const String & key) const
{
    return object_storage->exists(StoredObject(emuPath(key)));
}

String ObjectStorageBackend::emuRead(const String & key, Range range) const
{
    return readObjectRanged(*object_storage, emuPath(key), range);
}

Token ObjectStorageBackend::emuWrite(const String & key, const String & bytes, const ObjectMeta & meta)
{
    std::optional<ObjectAttributes> attrs;
    if (!meta.empty())
        attrs.emplace(meta.begin(), meta.end());   /// ObjectMeta is the same map type as ObjectAttributes
    auto buf = object_storage->writeObject(StoredObject(emuPath(key)), WriteMode::Rewrite, attrs);
    buf->write(bytes.data(), bytes.size());
    buf->finalize();

    const auto metadata = object_storage->tryGetObjectMetadata(emuPath(key), /*with_tags=*/false);
    return emuMintToken(key, metadata ? metadata->etag : String{}, /*just_wrote=*/true);
}

void ObjectStorageBackend::emuPublishBlobAtomically(const String & key, const String & bytes)
{
    if (object_storage->getType() != ObjectStorageType::Local)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "ObjectStorageBackend::publishBlob: atomic emulated publication requires local object storage");

    const String destination_object = emuPath(key);
    const String temporary_object = destination_object + ".publish-" + toString(UUIDHelpers::generateV4()) + ".tmp";
    const String root = object_storage->getCommonKeyPrefix();
    const String destination_path = resolvePathRelativelyToBase(destination_object, root);
    const String temporary_path = resolvePathRelativelyToBase(temporary_object, root);
    const auto existing_token_state = emu_token_state.find(key);

    try
    {
        auto out = object_storage->writeObject(StoredObject(temporary_object), WriteMode::Rewrite);
        out->write(bytes.data(), bytes.size());
        out->finalize();
        std::filesystem::rename(temporary_path, destination_path);
    }
    catch (...)
    {
        std::error_code cleanup_error;
        std::filesystem::remove(temporary_path, cleanup_error);
        throw;
    }

    /// Publication is transport-only and cannot HEAD to learn the replacement's ETag. Advancing an
    /// existing disambiguator is sufficient: if the next observation sees the same ETag, it returns
    /// a token distinct from the old incarnation; if the ETag changed, emuMintToken resets the state
    /// to that new ETag. With no existing state, this backend has issued no same-process stale token
    /// that needs fencing. The post-rename increment cannot allocate or throw.
    if (existing_token_state != emu_token_state.end())
        ++existing_token_state->second.second;
}

Token ObjectStorageBackend::emuObserveToken(const String & key)
{
    const auto metadata = object_storage->tryGetObjectMetadata(emuPath(key), /*with_tags=*/false);
    return emuMintToken(key, metadata ? metadata->etag : String{}, /*just_wrote=*/false);
}

Token ObjectStorageBackend::emuMintToken(const String & key, const String & etag, bool just_wrote)
{
    emuPruneTokenState(emuNowNs());

    /// Anomalous: the object storage reported no etag at all (LocalObjectStorage always does; this
    /// guards a hypothetical future/test double). Mint a fresh, UNPERSISTED value — never worse than
    /// the old counter for this case, but never masquerading as a real etag-derived identity.
    if (etag.empty())
        return Token{std::to_string(++emu_seq), TokenType::Emulated};

    auto it = emu_token_state.find(key);
    if (it != emu_token_state.end() && it->second.first == etag)
    {
        /// The etag has not advanced since the last token we minted for this key. For a read-only
        /// observation that is expected (the object simply has not changed) and the SAME value must be
        /// returned. For a just-completed WRITE it means this write's mtime landed in the same quantum
        /// as the previous incarnation's — two DIFFERENT incarnations must still never mint identical
        /// tokens, so bump a small per-key disambiguator (mtime-quantum guard, triage §3.18 19c step 4).
        if (just_wrote)
            ++it->second.second;
        const String value = it->second.second == 0 ? etag : etag + "#" + std::to_string(it->second.second);
        return Token{value, TokenType::Emulated};
    }

    /// The etag advanced (or this key is seen for the first time): the bare etag is the token, and any
    /// previous disambiguator is dropped — a genuinely new incarnation starts clean.
    emu_token_state[key] = {etag, 0};
    return Token{etag, TokenType::Emulated};
}

/// =========================================================================================
/// Backend interface
/// =========================================================================================

std::optional<GetResult> ObjectStorageBackend::get(const String & key, Range range)
{
    if (mode == Mode::Native)
    {
        auto hr = nativeHead(key);
        if (!hr)
            return std::nullopt;

        /// The object may be deleted between the HEAD above and the GET below (a GC or concurrent
        /// writer racing the read window). Catch the not-found signal and honor the `optional`
        /// contract — callers such as `Pool::loadShardDecoded` already handle a nullopt return and
        /// treat it as "raced a deletion, absent". Any other error (network, auth, corruption)
        /// propagates unchanged — fail-closed by construction.
        ///
        /// A REPLACEMENT racing the same window (HEAD observes token A, GET reads the bytes of a
        /// subsequently-written incarnation B) is likewise not a hazard: HEAD strictly precedes GET, so
        /// the returned token is never NEWER than the returned bytes — a mixed pair is always
        /// (bytes_newer, token_older), never the reverse. Every consumer of this token uses it as a
        /// conditional precondition (`casPut`/`putOverwrite`/`deleteExact`), which fails closed EXACTLY
        /// in the mixed case, so a stale token costs a retry, never lets a caller act on a
        /// bytes/token pair that never coexisted. This also covers `known_size`: content-addressed blob
        /// bodies are byte-identical across incarnations (a "replacement" only rotates envelope/token),
        /// mutable control objects are read-modify-CAS loops that re-validate on conflict, and write-once
        /// objects self-validate their contents on decode.
        GetResult gr;
        try
        {
            gr.bytes = readObjectRanged(*object_storage, key, range, hr->size);
        }
        catch (const std::exception & e)
        {
            if (isObjectNotFound(e))
                return std::nullopt;
            throw;
        }
        gr.token = hr->token;
        return gr;
    }

    std::lock_guard lock(emu_mutex);
    if (!emuExists(key))
        return std::nullopt;

    /// The emulated path holds emu_mutex across the exists-check and the read, so no concurrent
    /// caller in this process can delete the file in between. External deletion (e.g. a test teardown
    /// racing a read) is still handled: convert FILE_DOESNT_EXIST to nullopt rather than letting it
    /// escape as an unexplained exception.
    GetResult gr;
    try
    {
        gr.bytes = emuRead(key, range);
    }
    catch (const std::exception & e)
    {
        if (isObjectNotFound(e))
            return std::nullopt;
        throw;
    }
    gr.token = emuObserveToken(key);
    return gr;
}

std::optional<GetStreamResult> ObjectStorageBackend::getStream(const String & key, Range range)
{
    if (mode == Mode::Native)
    {
        auto hr = nativeHead(key);
        if (!hr)
            return std::nullopt;

        /// Same HEAD-then-read race as `get`: the object may be deleted between the HEAD above and the
        /// stream open below. Honor the `optional` contract on a not-found signal; any other error
        /// (network, auth, corruption) propagates unchanged — fail-closed by construction.
        GetStreamResult sr;
        try
        {
            sr.stream = openObjectRangedStream(*object_storage, key, range, hr->size);
        }
        catch (const std::exception & e)
        {
            if (isObjectNotFound(e))
                return std::nullopt;
            throw;
        }
        sr.token = hr->token;
        return sr;
    }

    std::lock_guard lock(emu_mutex);
    if (!emuExists(key))
        return std::nullopt;

    /// The emulated path holds emu_mutex across the exists-check and the stream open, matching `get`.
    /// External deletion still converts to nullopt rather than escaping as an unexplained exception.
    GetStreamResult sr;
    try
    {
        sr.stream = openObjectRangedStream(*object_storage, emuPath(key), range);
    }
    catch (const std::exception & e)
    {
        if (isObjectNotFound(e))
            return std::nullopt;
        throw;
    }
    sr.token = emuObserveToken(key);
    return sr;
}

HeadResult ObjectStorageBackend::head(const String & key)
{
    if (mode == Mode::Native)
    {
        auto hr = nativeHead(key);
        return hr ? *hr : HeadResult{};
    }

    std::lock_guard lock(emu_mutex);
    if (!emuExists(key))
        return HeadResult{};

    auto metadata = object_storage->tryGetObjectMetadata(emuPath(key), /*with_tags=*/false);
    /// A path that exists on the Local filesystem but yields no object metadata is a directory, not
    /// an object (`tryGetObjectMetadata` returns nullopt for a directory). HEAD must report it as
    /// not-an-object (exists=false) — otherwise existsFile/getStorageObjects treat a pool sub-dir (e.g.
    /// `store`, traversed by system.remote_data_paths) as a file and a later body read throws EISDIR.
    if (!metadata)
        return HeadResult{};
    HeadResult hr;
    hr.exists = true;
    hr.size = metadata->size_bytes;
    hr.attributes = ObjectMeta(metadata->attributes.begin(), metadata->attributes.end());
    hr.token = emuObserveToken(key);
    return hr;
}

/// See Backend::probeSentinelRaw / CasBackend.h's ProbeOutcome for the semantics this classifies.
SentinelProbeResult ObjectStorageBackend::probeSentinelRaw(const String & key)
{
    if (mode == Mode::Native)
    {
        try
        {
            /// `getObjectMetadata` (unlike `tryGetObjectMetadata`/`nativeHead`) is the THROWING raw-HEAD
            /// primitive — it does NOT collapse NO_SUCH_KEY/NO_SUCH_BUCKET/RESOURCE_NOT_FOUND into one
            /// `nullopt` before we get a chance to classify the S3 error. Its result is discarded here;
            /// only whether (and how) it throws matters — the body comes from `get` below.
            object_storage->getObjectMetadata(key, /*with_tags=*/false);

            /// The raw HEAD proved the key present. Delegate the body read to the existing `get`, which
            /// already HEADs again and reads — an extra round trip this authoritative, low-rate probe can
            /// afford, in exchange for reusing its already-correct HEAD→GET race handling. Kept INSIDE
            /// this try: a transient failure here must also classify Indeterminate, never escape unclassified.
            auto g = get(key);
            if (!g)
                return {ProbeOutcome::KeyAbsent, std::nullopt};   /// raced a deletion right after the raw HEAD
            return {ProbeOutcome::Present, std::move(g->bytes)};
        }
#if USE_AWS_S3
        catch (const S3Exception & e)
        {
            switch (e.getS3ErrorCode())
            {
                case Aws::S3::S3Errors::NO_SUCH_KEY:
                    return {ProbeOutcome::KeyAbsent, std::nullopt};
                case Aws::S3::S3Errors::RESOURCE_NOT_FOUND:
                    /// A HEAD response carries no body, so the SDK cannot parse a `NoSuchKey` `<Code>`
                    /// and instead derives this generic code straight from the HTTP 404 status (see
                    /// `isNotFoundError`, `src/IO/S3/getObjectInfo.cpp`) — this is what a REAL S3 HEAD
                    /// on an absent key actually throws. The container/key distinction is deliberately
                    /// NOT attempted here (a bodyless 404 cannot carry it).
                    return {ProbeOutcome::KeyAbsent, std::nullopt};
                case Aws::S3::S3Errors::NO_SUCH_BUCKET:
                    return {ProbeOutcome::ContainerAbsent, std::nullopt};
                case Aws::S3::S3Errors::ACCESS_DENIED:
                    return {ProbeOutcome::AccessDenied, std::nullopt};
                default:
                    /// Everything else (timeouts, 5xx, throttling, an unmodeled code) is inconclusive —
                    /// NEVER promoted to KeyAbsent, per the IAM permutation table in spec §2.
                    return {ProbeOutcome::Indeterminate, std::nullopt};
            }
        }
#endif
        catch (...)
        {
            return {ProbeOutcome::Indeterminate, std::nullopt};
        }
    }

    /// EmulatedSingleProcess (Local): stat the configured container directory FIRST — `emuExists`/`get`
    /// alone cannot distinguish "this key is absent" from "the whole pool directory is gone" (Local
    /// listing is best-effort and silently reports zero either way, see LocalObjectStorage::listObjects).
    try
    {
        if (!object_storage->existsOrHasAnyChild(emu_root))
            return {ProbeOutcome::ContainerAbsent, std::nullopt};

        auto g = get(key);
        if (!g)
            return {ProbeOutcome::KeyAbsent, std::nullopt};
        return {ProbeOutcome::Present, std::move(g->bytes)};
    }
    catch (...)
    {
        return {ProbeOutcome::Indeterminate, std::nullopt};
    }
}

/// Settings for a genuine Native conditional write. Mark the request for the typed conditional
/// dialect and, on a generation-token store, force a single PUT capped at
/// `conditional_single_put_cap`: GCS does not enforce the condition on multipart completion. Blob
/// publication never uses these settings; it remains an ordinary unconditional multipart-capable
/// write. CAS-mutable keys (shard manifests, gc/state, the registry) also skip the racy post-upload
/// existence/size check; a publish's manifest CAS was observed racing the GC fence there.
WriteSettings ObjectStorageBackend::conditionalWriteSettings() const
{
    WriteSettings ws;
    ws.object_storage_request_mode = ObjectStorageRequestMode::NativeConditional;
    if (native_token_type == TokenType::Generation)
    {
        ws.s3_force_single_part_upload = true;
        ws.s3_single_part_upload_max_bytes_override = conditional_single_put_cap;
    }
    ws.s3_check_objects_after_upload_override = false;
    /// Exactly one attempt at the WriteBufferFromS3 layer too: makeSinglepartUpload/
    /// completeMultipartUpload run their OWN retry loop above the S3 client, reissuing the identical
    /// (conditional!) request on NO_SUCH_KEY — a client-level override alone does not bound it. Plain
    /// size_t field, harmless (ignored) for a non-S3 write path.
    ws.s3_max_unexpected_write_error_retries_override = 1;
    /// Exactly one HTTP attempt for every conditional write: the object storage resolves the
    /// profile to its own single-attempt client. A backend that cannot honor it is rejected for
    /// writable Native mounts by checkConditionalWriteSingleAttemptSupport (fail closed).
    ws.object_storage_retry_profile = ObjectStorageRetryProfile::SingleAttempt;
    return ws;
}

/// See the declaration in the header for the policy. Centralizes the generation-vs-ETag attribution
/// decision for all successful conditional non-blob writes, including create-if-absent artifacts
/// and conditional replacements.
///
/// The strict Generation-dialect check below is gated on `etag.has_value()`, not merely on
/// `native_token_type`: `WriteBufferFromS3` unconditionally assigns `object_etag = outcome.GetResult().GetETag()`
/// on BOTH of its success paths -- `makeSinglepartUpload` (WriteBufferFromS3.cpp) and
/// `completeMultipartUpload` (WriteBufferFromS3.cpp) -- so a successful S3 write always leaves
/// `getResultObjectETag()` holding a value, empty string included; `has_value()` is exactly "this was
/// a real S3-style write response", the only case Step 7's "a missing x-goog-generation is an
/// exception" rule is ABOUT. `S3ObjectStorage::writeObject` returns that `WriteBufferFromS3` directly,
/// undecorated, so this holds for the whole CAS-over-S3 write path with no wrapping in between. A
/// backend with no write-time-token concept at all (local files, or a non-S3 `IObjectStorage`
/// exercising Generation dialect purely for a unit test, see
/// `CASBackendGeneration.StampedTokenTypeFollowsNativeKind`) reports `nullopt` structurally, not a
/// broken response, and keeps falling back to a fresh HEAD exactly like the ETag dialect. A future
/// change that wraps the returned write buffer in a decorator would need to re-derive or preserve this
/// chain -- `WriteBufferFromFileDecorator::getResultObjectETag` returns `nullopt` for a wrapped impl
/// that is not itself a `WriteBufferFromFileBase`, which would silently turn a hard failure back into
/// a HEAD fallback.
///
Token ObjectStorageBackend::tokenFromWriteResult(const String & key, const std::optional<String> & etag)
{
    if (native_token_type == TokenType::Generation && etag.has_value())
    {
        /// Validate the MINTED value, not the raw one: the HTTP boundary presents the generation
        /// through the SDK's ETag field and therefore quotes it, and `tokenForHead` is what strips
        /// that transport syntax. Validating before the strip would reject every real GCS write.
        /// The message still reports the raw arrival, since that is what needs diagnosing.
        const Token token = tokenForHead(*etag);
        if (!isValidGenerationTokenValue(token.value))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS on GCS: a conditional write to {} succeeded but its response carried no "
                "valid generation ({}) -- there is no follow-up HEAD to patch this over, so the write "
                "cannot be attributed to an incarnation",
                key, *etag);
        return token;
    }

    /// ETag dialect (and any backend with no write-time token at all, e.g. local files): unchanged
    /// pre-existing behavior -- an absent/empty value falls back to a fresh HEAD of `key`.
    if (etag && !etag->empty())
        return tokenForHead(*etag);

    auto hr = nativeHead(key);
    return hr ? hr->token : Token{};
}

PutResult ObjectStorageBackend::putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta)
{
    if (mode == Mode::Native)
    {
        WriteSettings ws = conditionalWriteSettings();
        ws.object_storage_write_if_none_match = "*";
        return nativeConditionalPut(key, bytes, ws, meta);
    }

    std::lock_guard lock(emu_mutex);
    if (emuExists(key))
        return {PutOutcome::PreconditionFailed, {}};

    return {PutOutcome::Done, emuWrite(key, bytes, meta)};
}

void ObjectStorageBackend::publishBlob(const BlobPublishRequest & request)
{
    if (const auto * streaming = std::get_if<StreamingBlobPublication>(&request.publication))
    {
        std::unique_ptr<ReadBuffer> payload = streaming->open_payload();
        if (!payload)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "ObjectStorageBackend::publishBlob: payload source for {} returned no reader",
                request.destination_key);

        if (mode != Mode::Native)
        {
            /// The emulated adapter's writes are whole-body operations. Serialize materialization so
            /// concurrent publications retain the existing one-body peak-memory bound.
            std::lock_guard publish_lock(emulatedBlobPublicationMutex());

            String body = streaming->fresh_envelope;
            blob_publication_detail::BlobPayloadCopyResult copy_result;
            {
                WriteBufferFromString out(body, AppendModeTag{});
                copy_result = blob_publication_detail::copyBlobPayloadBounded(*payload, out, streaming->payload_size);
                if (copy_result.exact(streaming->payload_size))
                    out.finalize();
                else
                    out.cancel();
            }

            if (!copy_result.exact(streaming->payload_size))
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "ObjectStorageBackend::publishBlob: source yielded {}{} payload bytes for {}, declared {} -- nothing was published",
                    copy_result.has_excess ? "more than " : "",
                    copy_result.copied,
                    request.destination_key,
                    streaming->payload_size);

            std::lock_guard lock(emu_mutex);
            emuPublishBlobAtomically(request.destination_key, body);
            return;
        }

        /// Ordinary unconditional rewrite: default request mode, retry profile, and multipart policy.
        /// In particular, generation stores are not restricted by the conditional single-PUT cap.
        auto out = object_storage->writeObject(
            StoredObject(request.destination_key),
            WriteMode::Rewrite,
            /*attributes=*/std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            WriteSettings{});
        out->write(streaming->fresh_envelope.data(), streaming->fresh_envelope.size());
        blob_publication_detail::BlobPayloadCopyResult copy_result;
        try
        {
            copy_result = blob_publication_detail::copyBlobPayloadBounded(*payload, *out, streaming->payload_size);
        }
        catch (...)
        {
            out->cancel();
            throw;
        }
        if (!copy_result.exact(streaming->payload_size))
        {
            out->cancel();
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "ObjectStorageBackend::publishBlob: source yielded {}{} payload bytes for {}, declared {} -- upload aborted, nothing published",
                copy_result.has_excess ? "more than " : "",
                copy_result.copied,
                request.destination_key,
                streaming->payload_size);
        }
        out->finalize();
        return;
    }

    const auto & staged = std::get<VerbatimStagedBlobPublication>(request.publication);
    if (mode != Mode::Native)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "ObjectStorageBackend::publishBlob: verbatim staged publication requires Native mode");

    WriteSettings write_settings;
    write_settings.object_storage_copy_mode = ObjectStorageCopyMode::NativeOnly;
    if (!object_storage->supportsCopyMode(write_settings.object_storage_copy_mode))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "ObjectStorageBackend::publishBlob: object storage {} does not support native-only same-store copy",
            object_storage->getName());

    object_storage->copyObject(
        StoredObject(staged.object_key),
        StoredObject(request.destination_key),
        getReadSettings(),
        write_settings);
}

PutResult ObjectStorageBackend::putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta)
{
    /// §3.18 №19: reject a wrong-dialect expected token before it ever reaches the wire (Native) or
    /// the emu compare (Emulated) — see mintingTypeMatches.
    if (!mintingTypeMatches(expected.type))
        return {PutOutcome::PreconditionFailed, {}};

    if (mode == Mode::Native)
    {
        WriteSettings ws = conditionalWriteSettings();
        ws.object_storage_write_if_match = expected.value;
        return nativeConditionalPut(key, bytes, ws, meta);
    }

    std::lock_guard lock(emu_mutex);
    if (!emuExists(key))
        return {PutOutcome::PreconditionFailed, {}};
    if (!tokenMatches(emuObserveToken(key), expected))
        return {PutOutcome::PreconditionFailed, {}};

    return {PutOutcome::Done, emuWrite(key, bytes, meta)};
}

CasResult ObjectStorageBackend::casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta)
{
    /// §3.18 №19: a create-if-absent CAS (expected == nullopt) has no token to validate; only the
    /// swap form carries one, and it must match this backend's own minting dialect before anything
    /// else runs.
    if (expected.has_value() && !mintingTypeMatches(expected->type))
        return {CasOutcome::Conflict, {}};

    if (mode == Mode::Native)
    {
        WriteSettings ws = conditionalWriteSettings();
        if (expected.has_value())
            ws.object_storage_write_if_match = expected->value;
        else
            ws.object_storage_write_if_none_match = "*";

        /// The PUT-side outcomes (Done / PreconditionFailed) collapse onto CAS outcomes 1:1: a lost
        /// condition — whether a mismatched If-Match or a 404 on an If-Match PUT — is a Conflict.
        PutResult put = nativeConditionalPut(key, bytes, ws, meta);
        return put.outcome == PutOutcome::Done
            ? CasResult{CasOutcome::Committed, put.token}
            : CasResult{CasOutcome::Conflict, {}};
    }

    std::lock_guard lock(emu_mutex);
    const bool exists = emuExists(key);

    if (!expected.has_value())
    {
        if (exists)
            return {CasOutcome::Conflict, {}};
    }
    else
    {
        if (!exists)
            return {CasOutcome::Conflict, {}};
        if (!tokenMatches(emuObserveToken(key), *expected))
            return {CasOutcome::Conflict, {}};
    }

    return {CasOutcome::Committed, emuWrite(key, bytes, meta)};
}

DeleteOutcome ObjectStorageBackend::deleteExact(const String & key, const Token & token)
{
    /// §3.18 №19: same local dialect guard as putOverwrite/casPut — never forward a foreign-dialect
    /// value as the removeObjectIfTokenMatches argument.
    if (!mintingTypeMatches(token.type))
    {
        DeleteOutcome d;
        d.kind = DeleteOutcome::Kind::TokenMismatch;
        return d;
    }

    if (mode == Mode::Native)
    {
        /// `removeObjectIfTokenMatches` maps onto `DeleteOutcome` one-to-one. `NOT_IMPLEMENTED` from a
        /// backend that does not enforce conditional removal propagates — fail-closed by construction.
        auto result = object_storage->removeObjectIfTokenMatches(StoredObject(key), token.value);
        DeleteOutcome d;
        d.created_delete_marker = result.created_delete_marker;
        switch (result.outcome)
        {
            case ConditionalRemoveOutcome::Removed:
                d.kind = DeleteOutcome::Kind::Deleted;
                break;
            case ConditionalRemoveOutcome::TokenMismatch:
                d.kind = DeleteOutcome::Kind::TokenMismatch;
                break;
            case ConditionalRemoveOutcome::NotFound:
                d.kind = DeleteOutcome::Kind::NotFound;
                break;
        }
        return d;
    }

    std::lock_guard lock(emu_mutex);
    DeleteOutcome d;
    if (!emuExists(key))
    {
        d.kind = DeleteOutcome::Kind::NotFound;
        return d;
    }
    if (!tokenMatches(emuObserveToken(key), token))
    {
        d.kind = DeleteOutcome::Kind::TokenMismatch;
        return d;
    }

    object_storage->removeObjectIfExists(StoredObject(emuPath(key)));
    /// Keep the deleted incarnation's last-minted etag around ONLY while a same-mtime-quantum
    /// collision with an immediate recreate is still possible (emuMintToken) — once it is
    /// comfortably old, erase it so `emu_token_state` does not grow for the lifetime of the backend
    /// instance (codex-review-triage §3.18, Important #1).
    if (auto it = emu_token_state.find(key); it != emu_token_state.end())
    {
        const uint64_t now_ns = emuNowNs();
        if (etagComfortablyInThePast(it->second.first, now_ns))
            emu_token_state.erase(it);
        else
            emu_token_expiry.push_back(EmuTokenExpiry{now_ns, key, it->second});
    }
    d.kind = DeleteOutcome::Kind::Deleted;
    return d;
}

ListPage ObjectStorageBackend::list(const String & prefix, const String & cursor, size_t limit)
{
    /// Use the lazy object-storage iterator instead of `listObjects(..., max_keys=0)`: the latter
    /// materialized the whole prefix, then sliced client-side, so a paginated walk re-fetched the full
    /// subtree for every page. The backend cursor is "last key returned" (exclusive on resume).
    ///
    /// Some backends ignore `start_after`; filtering `key <= cursor` keeps the contract correct there,
    /// only losing the resume optimization. S3 honors `start_after` and avoids the hot-path re-scan.
    if (limit == 0)
        return {};

    const String physical_prefix = (mode == Mode::EmulatedSingleProcess) ? emuPath(prefix) : prefix;
    const String strip = (mode == Mode::EmulatedSingleProcess) ? emuPath("") : String{};
    if (mode == Mode::EmulatedSingleProcess)
    {
        RelativePathsWithMetadata children;
        object_storage->listObjects(physical_prefix, children, /*max_keys=*/0);

        /// Hold emu_mutex across the whole scan: emuMintToken below reads/updates emu_token_state, the
        /// same per-key state get/head/put*/delete* mutate under this lock (see the "caller holds
        /// emu_mutex" contract on the private emu* helpers).
        std::lock_guard lock(emu_mutex);

        std::vector<ListedKey> all;
        all.reserve(children.size());
        for (const auto & child : children)
        {
            if (!child->relative_path.starts_with(physical_prefix))
                continue;
            ListedKey lk;
            lk.key = child->relative_path.substr(strip.size());
            lk.size = child->metadata ? child->metadata->size_bytes : 0;
            /// §3.18 №18: mint DIRECTLY as TokenType::Emulated — do NOT call tokenForList, which always
            /// stamps native_token_type (ETag/Generation) regardless of mode and would surface a token
            /// of the wrong dialect for every Emulated consumer (head/get mint Emulated).
            if (child->metadata)
                lk.token = emuMintToken(lk.key, child->metadata->etag, /*just_wrote=*/false);
            all.push_back(std::move(lk));
        }
        std::sort(all.begin(), all.end(), [](const ListedKey & a, const ListedKey & b) { return a.key < b.key; });

        ListPage page;
        auto all_it = cursor.empty()
            ? std::lower_bound(all.begin(), all.end(), prefix, [](const ListedKey & a, const String & s) { return a.key < s; })
            : std::upper_bound(all.begin(), all.end(), cursor, [](const String & s, const ListedKey & a) { return s < a.key; });
        while (all_it != all.end() && page.keys.size() < limit)
        {
            page.keys.push_back(*all_it);
            ++all_it;
        }
        if (!page.keys.empty() && all_it != all.end())
            page.next_cursor = page.keys.back().key;
        return page;
    }

    const std::optional<String> start_after = cursor.empty()
        ? std::nullopt
        : std::optional<String>(cursor);

    ListPage page;
    auto it = object_storage->iterate(physical_prefix, /*max_keys=*/0, /*with_tags=*/false, start_after);
    for (; it->isValid(); it->next())
    {
        const auto child = it->current();
        if (!child->relative_path.starts_with(physical_prefix))
            continue;

        ListedKey lk;
        lk.key = child->relative_path.substr(strip.size());
        if (!cursor.empty() && lk.key <= cursor)
            continue;

        lk.size = child->metadata ? child->metadata->size_bytes : 0;
        /// Surface the per-key incarnation token (matching what `head` would return, see above) so the
        /// `supportsListTokens() == true` capability is honest. A listing without an etag leaves the
        /// token unset, which GC discover treats as Read (fail closed). The supportsListTokens()+
        /// empty-etag gate now lives in tokenForList.
        if (child->metadata)
            lk.token = tokenForList(child->metadata->etag);

        if (page.keys.size() == limit)
        {
            page.next_cursor = page.keys.back().key;
            break;
        }
        page.keys.push_back(std::move(lk));
    }

    return page;
}

}
