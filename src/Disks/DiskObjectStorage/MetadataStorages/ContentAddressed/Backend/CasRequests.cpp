#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>

#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <Common/LoggingHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>

#include "config.h"

#if USE_AWS_S3
#include <IO/S3Common.h>
#endif

#include <Poco/Exception.h>

#include <fmt/format.h>

#include <array>
#include <ctime>
#include <limits>
#include <utility>

namespace ProfileEvents
{
    extern const Event CASRequestAttempt;
    extern const Event CASRequestReissue;
    extern const Event CASRequestConflictPause;
    extern const Event CASRequestResolveRead;
    extern const Event CASRequestGaveUp;
    extern const Event CASRequestRefused;
    extern const Event CASRequestFenceLostPostWrite;
    extern const Event CASRequestConnectFailureHint;
    extern const Event CASRequestFirstAttemptFuse;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CAS_DELETE_MARKER;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::Cas
{

namespace detail
{

void recordAttempt()
{
    ProfileEvents::increment(ProfileEvents::CASRequestAttempt);
}

void recordReissue()
{
    ProfileEvents::increment(ProfileEvents::CASRequestReissue);
}

void recordConflictPause()
{
    ProfileEvents::increment(ProfileEvents::CASRequestConflictPause);
}

void recordFirstAttemptFuse()
{
    ProfileEvents::increment(ProfileEvents::CASRequestFirstAttemptFuse);
}

}

namespace
{
/// Shared by the two entry points below so the log line and the exception's message text can never
/// drift apart. Rate-limited (not per-distinct-`why` -- `LogSeriesLimiter` keys on the LOGGER NAME
/// only, so under a sustained outage where `why` keeps changing slightly, only the first message in
/// each window prints; this is the intended throttle, not a bug). Warning-level visibility is
/// intentional: this condition is expected to self-heal (the caller retries), but an operator watching
/// CAS logs directly should see it without having to know to look at system.replication_queue.
void logCasWriteRetryLater(const String & why)
{
    LogSeriesLimiter log(getLogger("CasWriteRetryLater"), /*allowed_count=*/1, /*interval_s=*/30);
    LOG_WARNING(log, "CAS write could not be committed ({}); retrying later", why);
}
}

[[noreturn]] void throwCasWriteRetryLater(const String & why)
{
    logCasWriteRetryLater(why);
    throw Exception(ErrorCodes::NETWORK_ERROR, "CAS write could not be committed ({}); retrying later", why);
}

std::exception_ptr makeCasWriteRetryLaterExceptionPtr(const String & why)
{
    logCasWriteRetryLater(why);
    return std::make_exception_ptr(
        Exception(ErrorCodes::NETWORK_ERROR, "CAS write could not be committed ({}); retrying later", why));
}

[[noreturn]] void throwCasTransientUnavailable(const String & subject, const String & condition)
{
    /// The code is coarse (it shares a `system.errors` row with socket failures), so the MESSAGE must
    /// carry the whole truth: which CA condition refused, and that the refusal is a state rather than
    /// damage. Consumers key on the code; operators read this line.
    ///
    /// The shared suffix carries ONLY the classification, because that is the one claim true at every
    /// site: retry-later is right even where the condition may turn out terminal, since the next attempt
    /// re-decides against fresh state. Any promise about HOW the condition clears belongs in `condition`,
    /// where the site that can actually prove it makes it -- `checkFenceOrThrow` provably cannot.
    throw Exception(ErrorCodes::NETWORK_ERROR,
        "{} -- {}; TRANSIENT unavailability, not damage", subject, condition);
}

namespace
{

/// `CLOCK_BOOTTIME` milliseconds -- the clock a mount lease deadline is expressed on, reproduced here
/// rather than shared because `Backend/` does not depend on the mount plane.
uint64_t bootClockMs()
{
    return clock_gettime_ns(CLOCK_BOOTTIME) / 1000000;
}

uint64_t saturatingAdd(uint64_t lhs, uint64_t rhs)
{
    return lhs > std::numeric_limits<uint64_t>::max() - rhs ? std::numeric_limits<uint64_t>::max() : lhs + rhs;
}

}

/// The failure class a fresh credential could fix, named here rather than taken from
/// `S3Exception::isAccessTokenExpiredError`, which also fires on `S3Errors::UNKNOWN` -- the SDK's code
/// for EVERY error it does not model. Borrowing it would put throttling codes an S3-compatible store
/// reports under a non-AWS name into the credential class, and would carve a real access denial out of
/// `isDefinitelyRefusedWrite` so the write wedges its caller instead of being refused. `UNKNOWN` is
/// therefore never matched by code alone; a store that spells the error out by name still matches.
///
/// Declared in the header: a caller whose OWN loop makes the next physical attempt has to tell this
/// class apart from the rest of `isDefinitelyRefusedWrite`.
bool isRefreshableCredentialError([[maybe_unused]] const std::exception & e)
{
#if USE_AWS_S3
    const auto * s3 = dynamic_cast<const S3Exception *>(&e);
    if (!s3)
        return false;
    const Aws::S3::S3Errors code = s3->getS3ErrorCode();
    if (code == Aws::S3::S3Errors::INVALID_ACCESS_KEY_ID || code == Aws::S3::S3Errors::ACCESS_DENIED
        || code == Aws::S3::S3Errors::INVALID_SIGNATURE || code == Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID)
        return true;
    const String & name = s3->getExceptionName();
    return name == "ExpiredToken" || name == "InvalidToken" || name == "InvalidAccessKeyId"
        || name == "SignatureDoesNotMatch" || name == "AccessDenied" || name == "AccountProblem";
#else
    return false;
#endif
}

namespace
{

/// The store's authoritative answer that there is nothing at the key -- a fact about the OBJECT, which
/// reissuing only replays. Deliberately narrower than "not retryable": a missing bucket or an
/// unmodeled name is an answer about reaching the store, which an S3-compatible store gives
/// transiently when it misroutes a request, so it stays in the ambiguous class and is reissued until
/// the deadline. The credential codes never reach here -- `refreshAndClassifyReadFault` consumes them
/// first.
bool isAuthoritativeAbsence([[maybe_unused]] const std::exception & e)
{
#if USE_AWS_S3
    if (const auto * s3 = dynamic_cast<const S3Exception *>(&e))
    {
        const Aws::S3::S3Errors code = s3->getS3ErrorCode();
        return code == Aws::S3::S3Errors::NO_SUCH_KEY || code == Aws::S3::S3Errors::NO_SUCH_UPLOAD;
    }
#endif
    return false;
}

GaveUp::Source sourceFor(const Retry::Bound & bound)
{
    return bound.lease_bound ? GaveUp::Source::Lease : GaveUp::Source::Policy;
}

/// Could the precondition this write was built with still be met by what the resolve read saw? A
/// create needs the key absent; a replace needs the incarnation it named to still be current.
bool preconditionStillSatisfiable(const Observation & seen, const std::optional<Etag> & expected)
{
    return std::visit(detail::Overload{
        /// The read itself failed, so it proved nothing either way and an ambiguous attempt may still
        /// be alive. Reporting a conflict on it would name an occupant nobody observed.
        [](const NotObserved &) { return true; },
        [&](const ProvenAbsent &) { return !expected.has_value(); },
        [&](const Meta & m) { return expected.has_value() && m.etag == *expected; },
        [&](const Object & o) { return expected.has_value() && o.etag == *expected; }},
        seen);
}

/// Drop the body from an observation the write engine had to fetch to prove whose bytes were at the
/// key. The presence-only loop is defined by what it reports, so the demotion happens on its results
/// rather than being trusted to every branch that builds one.
Observation withoutBody(Observation seen)
{
    if (const auto * obj = std::get_if<Object>(&seen))
        return Meta{obj->bytes.size(), obj->etag};
    return seen;
}

WriteResult withoutBody(WriteResult result)
{
    if (auto * conflict = std::get_if<Conflict>(&result))
        conflict->seen = withoutBody(std::move(conflict->seen));
    else if (auto * declined = std::get_if<Declined>(&result))
        declined->seen = withoutBody(std::move(declined->seen));
    else if (auto * gave_up = std::get_if<GaveUp>(&result))
        gave_up->last_seen = withoutBody(std::move(gave_up->last_seen));
    return result;
}

}

bool isDeterministicLocalFailure(int code)
{
    return code == ErrorCodes::LOGICAL_ERROR || code == ErrorCodes::NOT_IMPLEMENTED
        || code == ErrorCodes::BAD_ARGUMENTS || code == ErrorCodes::CORRUPTED_DATA;
}

bool isDefinitelyRefusedWrite([[maybe_unused]] const std::exception & e)
{
#if USE_AWS_S3
    if (const auto * s3 = dynamic_cast<const S3Exception *>(&e))
        /// The refresh class is included rather than left to overlap: the two name lists agree, but
        /// `InvalidSignature` carries a code `isAccessDeniedError` does not match, and an error that is
        /// refreshable without being refusable would spend the whole deadline whenever no refresh is
        /// available -- which is every CAS disk today.
        return S3::isMalformedRequestError(*s3) || S3::isEntityTooLargeError(*s3)
            || S3::isAccessDeniedError(*s3) || isRefreshableCredentialError(e);
#endif
    return false;
}

bool isConnectFailureHint([[maybe_unused]] const std::exception & e)
{
#if USE_AWS_S3
    const auto * s3 = dynamic_cast<const S3Exception *>(&e);
    if (!s3 || s3->getS3ErrorCode() != Aws::S3::S3Errors::NETWORK_CONNECTION)
        return false;
    /// This repository's Poco (`SocketImpl::error`, `SocketImpl::connect`) is the source of every text.
    static constexpr std::array<std::string_view, 5> texts{
        "Cannot assign requested address", "Connection refused", "No route to host",
        "Network is unreachable", "connect timed out"};
    const std::string_view message = s3->message();
    for (std::string_view text : texts)
        if (message.find(text) != std::string_view::npos)
            return true;
#endif
    return false;
}

bool isFirstAttemptFuseTimeout([[maybe_unused]] const std::exception & e, [[maybe_unused]] size_t attempt_no)
{
#if USE_AWS_S3
    /// The connect-failure hint is checked FIRST: `connect timed out` belongs to that classifier, and
    /// an attempt whose text matches both stays a hint, reissued without a preceding settle read.
    if (attempt_no != 1 || isConnectFailureHint(e))
        return false;
    const auto * s3 = dynamic_cast<const S3Exception *>(&e);
    if (!s3 || s3->getS3ErrorCode() != Aws::S3::S3Errors::NETWORK_CONNECTION)
        return false;
    const std::string_view message = s3->message();
    return message.find("Timeout") != std::string_view::npos;
#else
    return false;
#endif
}

CasRequests::CasRequests(BackendPtr backend_, Fence fence_,
                         std::function<uint64_t()> now_ms_, std::function<void(uint64_t)> sleep_ms_,
                         CasHotKeys * hot_keys_)
    : backend(std::move(backend_))
    , fence(std::move(fence_))
    , now_ms(now_ms_ ? std::move(now_ms_) : std::function<uint64_t()>(bootClockMs))
    , sleep_ms(sleep_ms_ ? std::move(sleep_ms_) : std::function<void(uint64_t)>(sleepForMilliseconds))
    , attempt_reservation_ms(backend->attemptEnvelopeMs())
    , own_hot_keys(hot_keys_ ? nullptr : std::make_unique<CasHotKeys>(0))
    , hot_keys(hot_keys_ ? hot_keys_ : own_hot_keys.get())
{
}

void CasRequests::setNowFnForTest(std::function<uint64_t()> now_ms_)
{
    now_ms = now_ms_ ? std::move(now_ms_) : std::function<uint64_t()>(bootClockMs);
}

void CasRequests::setSleepFnForTest(std::function<void(uint64_t)> sleep_ms_)
{
    sleep_ms = sleep_ms_ ? std::move(sleep_ms_) : std::function<void(uint64_t)>(sleepForMilliseconds);
}

CasOperation CasRequests::admit(Liveness liveness)
{
    return CasOperation(*this, fence.generation(), std::move(liveness));
}

CasOperation CasRequests::resume(uint64_t admitted_generation, Liveness liveness)
{
    return CasOperation(*this, admitted_generation, std::move(liveness));
}

std::optional<Etag> CasRequests::tryMint(const String & key, String value) const
{
    if (!isIncarnationValue(backend->dialect(), value))
        return std::nullopt;
    return Etag(backend->backendId(), key, backend->dialect(), std::move(value));
}

Etag CasRequests::mint(const String & key, String value) const
{
    if (auto minted = tryMint(key, value))
        return std::move(*minted);
    throw Exception(ErrorCodes::CORRUPTED_DATA,
        "CAS: the store answered for '{}' with a value '{}' that is not a valid incarnation", key, value);
}

const String & CasRequests::valueFor(const String & key, const Etag & inc) const
{
    if (inc.key() != key || inc.backendId() != backend->backendId())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS: an incarnation of '{}' observed on backend {} cannot be the precondition for '{}' on backend {}",
            inc.key(), inc.backendId(), key, backend->backendId());
    return inc.value();
}

namespace
{

/// The one mapping from a refused admission to the exception a read-class caller sees. The request
/// gate and the streamed body below both refuse through it, so a body refused mid-transfer reads
/// exactly like an open refused before it: `NoBudget` is the retry-later class, everything else the
/// tripped-fence class. Free, not a member: the body's copy must not reference an operation.
[[noreturn]] void throwReadRefused(Fence::Admit admit, std::string_view verb, const String & subject,
                                   std::string_view when)
{
    if (admit == Fence::Admit::NoBudget)
        throwCasWriteRetryLater(fmt::format("{} of '{}': no lease budget {}", verb, subject, when));
    throwCasTransientUnavailable(fmt::format("CAS {} of '{}'", verb, subject),
                                 fmt::format("mount fence tripped {}", when));
}

/// The body of a streamed object, re-admitted at every refill. `Backend::stream` bounds only the
/// open; the SDK reads the body at the consumer's pace, under the storage's ordinary settings, long
/// after the attempt that opened it returned -- so a fold parked in a multi-gigabyte run body would
/// outlive the fence that refused every other request of its operation. The check is the operation's
/// own admission, asked once per SDK buffer, and its refusal is the exception a refused open produces.
///
/// The predicate is held BY VALUE -- the fence's `admit` closure, the admitted generation, the
/// caller's liveness -- never through the operation: `CasOperation::stream` returns a buffer that can
/// outlive the operation object (S3 staging opens its stream under a local mount-plane operation and
/// hands the buffer to the backend). Modelled on `LimitReadBuffer`: no byte is copied, the SDK's
/// window is exposed as this buffer's own.
class AdmittedBodyReadBuffer : public ReadBuffer
{
public:
    AdmittedBodyReadBuffer(std::unique_ptr<ReadBuffer> in_, String key_,
                           std::function<Fence::Admit(uint64_t, uint64_t)> admit_,
                           uint64_t admitted_generation_, Liveness liveness_)
        /// The open already loaded a window (`Backend::stream` forces the first GET): adopt it, so
        /// the first refill this buffer asks for is the SECOND window and the SDK buffer is never
        /// asked to advance over pending data.
        : ReadBuffer(in_->position(), in_->available(), 0)
        , in(std::move(in_))
        , key(std::move(key_))
        , admit(std::move(admit_))
        , admitted_generation(admitted_generation_)
        , liveness(std::move(liveness_))
    {
    }

private:
    bool nextImpl() override
    {
        /// Let the SDK buffer account the bytes the consumer took from the shared window.
        in->position() = position();

        Fence::Admit verdict = admit(admitted_generation, 0);
        if (verdict == Fence::Admit::Ok && liveness && !liveness())
            verdict = Fence::Admit::LostOrRearmed;
        if (verdict != Fence::Admit::Ok)
            throwReadRefused(verdict, "stream body", key, "mid-body");

        if (!in->next())
        {
            BufferBase::set(in->position(), 0, 0);
            return false;
        }
        BufferBase::set(in->position(), in->available(), 0);
        return true;
    }

    std::unique_ptr<ReadBuffer> in;
    String key;
    std::function<Fence::Admit(uint64_t, uint64_t)> admit;
    uint64_t admitted_generation;
    Liveness liveness;
};

}

CasOperation::Gate CasOperation::gate(uint64_t needed_ms) const
{
    switch (owner.fence.admit(admitted_generation, needed_ms))
    {
        case Fence::Admit::LostOrRearmed: return Gate::FenceLost;
        case Fence::Admit::NoBudget:      return Gate::NoBudget;
        case Fence::Admit::Ok: break;
    }
    /// The caller's own facts are the second half of admission, and the engine does not need to know
    /// which of the two refused: a stopping task and a lost lease end the operation the same way.
    if (liveness && !liveness())
        return Gate::FenceLost;
    return Gate::Ok;
}

uint64_t CasOperation::reservedFor(uint64_t sleep_ms, uint32_t envelopes) const
{
    uint64_t total = sleep_ms;
    for (uint32_t i = 0; i < envelopes; ++i)
        total = saturatingAdd(total, owner.attempt_reservation_ms);
    return total;
}

Retry CasOperation::freeze(const Retry & policy) const
{
    if (policy.policy_deadline_ms)
        return policy;
    Retry frozen = policy;
    frozen.policy_deadline_ms = saturatingAdd(owner.now_ms(), policy.window_ms);
    return frozen;
}

bool CasOperation::fits(uint64_t needed_ms, const Retry::Bound & bound) const
{
    /// Strict at the boundary. A backend with no attempt timeout reserves 0 and full jitter can draw a
    /// 0 sleep, so a `needed <= remaining` test would keep issuing requests at and past the deadline --
    /// the one thing "no request after the boundary" promises never happens.
    const uint64_t now = owner.now_ms();
    if (now >= bound.deadline_ms)
        return false;
    return needed_ms <= bound.deadline_ms - now;
}

bool CasOperation::refreshAndClassifyReadFault(const std::exception & e, bool & refresh_attempted, bool & refreshed)
{
    if (const auto * db_e = dynamic_cast<const Exception *>(&e); db_e && isDeterministicLocalFailure(db_e->code()))
        return true;
    /// A local failure -- a bad allocation, a logic error raised inside the attempt -- is not a
    /// transport fault, and reissuing it would spend the whole deadline replaying the same bug. Every
    /// exception the transport raises is a `Poco::Exception`, `DB::Exception` included.
    if (!dynamic_cast<const Poco::Exception *>(&e))
        return true;
    /// A credential failure gets ONE refresh per call -- the storage hands back a fresh client every
    /// time it is asked, so refreshing per attempt would reissue a permanent denial to the deadline.
    /// Without new credentials nothing would sign differently, so a read propagates rather than
    /// spending its policy on a request that cannot start succeeding.
    if (isRefreshableCredentialError(e))
    {
        if (refresh_attempted)
            return true;
        refresh_attempted = true;
        refreshed = owner.backend->refreshCredentials();
        return !refreshed;
    }
    /// The store's own answer decides. A refusal it proved never applied, and an authoritative absence,
    /// both replay identically; everything else -- a throttle, a 5xx, a missing bucket, an unmodeled
    /// name an S3-compatible store reports -- may still be transient.
    return isDefinitelyRefusedWrite(e) || isAuthoritativeAbsence(e);
}

void CasOperation::giveUpReadFenceLost(std::string_view verb, const String & subject, std::string_view when)
{
    last_read_stop = ReadStop::FenceLost;
    throwReadRefused(Fence::Admit::LostOrRearmed, verb, subject, when);
}

void CasOperation::giveUpReadNoBudget(std::string_view verb, const String & subject, std::string_view what)
{
    last_read_stop = ReadStop::NoBudgetLease;
    throwReadRefused(Fence::Admit::NoBudget, verb, subject, what);
}

void CasOperation::giveUpReadDeadline(std::string_view verb, const String & subject,
                                      const Retry::Bound & bound, uint32_t attempts_made)
{
    last_read_stop = ReadStop::PolicyExhausted;
    throwCasWriteRetryLater(fmt::format("{} of '{}': gave up at the {} deadline after {} attempt(s)",
                                        verb, subject, bound.lease_bound ? "lease" : "policy", attempts_made));
}

std::optional<Object> CasOperation::readUnder(const String & key, const Retry & policy, const Retry::Bound & bound)
{
    return readLoop("read", key, policy, bound, [&](auto & access) -> std::optional<Object>
    {
        auto raw = owner.backend->read(key, access);
        if (!raw)
            return std::nullopt;
        return Object{std::move(raw->bytes), owner.mint(key, std::move(raw->value))};
    });
}

std::optional<Meta> CasOperation::headUnder(const String & key, const Retry & policy, const Retry::Bound & bound)
{
    return readLoop("head", key, policy, bound, [&](auto & access) -> std::optional<Meta>
    {
        auto raw = owner.backend->head(key, access);
        if (!raw)
            return std::nullopt;
        return Meta{raw->size, owner.mint(key, std::move(raw->value))};
    });
}

ListPage CasOperation::listUnder(const String & prefix, const String & cursor, size_t limit,
                                const Retry & policy, const Retry::Bound & bound)
{
    return readLoop("list", prefix, policy, bound, [&](auto & access)
    {
        Backend::RawListPage raw = owner.backend->list(prefix, cursor, limit, access);
        ListPage page;
        page.next_cursor = std::move(raw.next_cursor);
        page.keys.reserve(raw.keys.size());
        for (auto & listed : raw.keys)
        {
            ListedKey entry{std::move(listed.key), listed.size, std::nullopt};
            if (listed.value)
                entry.etag = owner.mint(entry.key, std::move(*listed.value));
            page.keys.push_back(std::move(entry));
        }
        return page;
    });
}

Removal CasOperation::removeUnder(const String & key, const String & expected_value,
                                  const Retry & policy, const Retry::Bound & bound)
{
    const Backend::RawRemoval raw = readLoop("remove", key, policy, bound, [&](auto & access)
    {
        return owner.backend->remove(key, expected_value, access);
    });
    switch (raw)
    {
        case Backend::RawRemoval::Removed:  return Removal::Removed;
        case Backend::RawRemoval::Gone:     return Removal::Gone;
        case Backend::RawRemoval::Mismatch: return Removal::Mismatch;
        case Backend::RawRemoval::DeleteMarker: break;
    }
    /// Thrown outside the attempt loop: a versioned bucket answers this way every time, so reissuing
    /// would spend the whole deadline to be told the same thing.
    throw Exception(ErrorCodes::CAS_DELETE_MARKER,
        "CAS remove of '{}' archived a noncurrent version instead of reclaiming the object: "
        "the bucket has object versioning enabled", key);
}

std::optional<Object> CasOperation::read(const String & key, const Retry & policy)
{
    return readUnder(key, policy, policy.bind(owner.now_ms()));
}

std::optional<Meta> CasOperation::head(const String & key, const Retry & policy)
{
    return headUnder(key, policy, policy.bind(owner.now_ms()));
}

ListPage CasOperation::list(const String & prefix, const String & cursor, size_t limit, const Retry & policy)
{
    return listUnder(prefix, cursor, limit, policy, policy.bind(owner.now_ms()));
}

void CasOperation::forEachListedKey(const String & prefix, const ListedKeyFn & fn, const Retry & per_page,
                                    size_t page_limit, const std::function<void()> & on_page_fetched)
{
    String cursor;
    for (;;)
    {
        ListPage page = list(prefix, cursor, page_limit, per_page);
        if (on_page_fetched)
            on_page_fetched();
        for (const ListedKey & entry : page.keys)
            if (!fn(entry))
                return;
        if (page.next_cursor.empty())
            return;
        cursor = std::move(page.next_cursor);
    }
}

void CasOperation::removeManyWriteOnce(const std::vector<WriteOnceKey> & keys, const Retry & policy)
{
    if (keys.size() > kBulkDeleteMaxKeys)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS removeManyWriteOnce: {} keys in one chunk, the limit is {}; the consumer chunks its input",
            keys.size(), kBulkDeleteMaxKeys);
    if (keys.empty())
        return;
    const Retry::Bound bound = policy.bind(owner.now_ms());
    const String subject = fmt::format("{} (+{} keys)", keys.front().str(), keys.size() - 1);
    readLoop("removeManyWriteOnce", subject, policy, bound, [&](auto & access)
    {
        owner.backend->removeManyWriteOnce(keys, access);
        return true;
    });
}

Removal CasOperation::remove(const String & key, const Etag & seen, const Retry & policy)
{
    return removeUnder(key, owner.valueFor(key, seen), policy, policy.bind(owner.now_ms()));
}

Removal CasOperation::removeCurrent(const String & key, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    for (uint32_t attempt = 1;; ++attempt)
    {
        const std::optional<Meta> seen = headUnder(key, policy, bound);
        if (!seen)
            return Removal::Gone;
        const Removal removed = removeUnder(key, owner.valueFor(key, seen->etag), policy, bound);
        if (removed != Removal::Mismatch)
            return removed;

        /// A `Mismatch` is a VALUE, so it never reaches the fault check inside the two loops above: this
        /// verb has to honour `once` itself, and its contract is that it never hands a `Mismatch` back.
        /// The message says what actually stopped it, which is the policy and not the deadline.
        if (policy.single_attempt)
            throwCasWriteRetryLater(fmt::format(
                "removeCurrent of '{}': the observed incarnation was replaced and the policy allows no reissue", key));

        /// Another incarnation became current between the observation and the delete. Re-observe, paced
        /// like every other reissue: the reservation covers the next `head` and the `remove` after it.
        const uint64_t pause_ms = Retry::backoff(attempt);
        const uint64_t needed = reservedFor(pause_ms, 2);
        switch (gate(needed))
        {
            case Gate::FenceLost: giveUpReadFenceLost("removeCurrent", key, "before the reissue");
            case Gate::NoBudget:  giveUpReadNoBudget("removeCurrent", key, "for the reissue");
            case Gate::Ok: break;
        }
        if (!fits(needed, bound))
            giveUpReadDeadline("removeCurrent", key, bound, attempt);
        detail::recordReissue();
        owner.sleep_ms(pause_ms);
    }
}

SentinelProbeResult CasOperation::probeSentinel(const String & key, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    bool refresh_attempted = false;
    for (uint32_t attempt = 1;; ++attempt)
    {
        const uint64_t reservation = reservedFor(0, 1);
        switch (gate(reservation))
        {
            case Gate::FenceLost: giveUpReadFenceLost("probeSentinel", key, "before the request");
            case Gate::NoBudget:  giveUpReadNoBudget("probeSentinel", key, "for one more request");
            case Gate::Ok: break;
        }
        /// Before the first attempt nothing has been probed, so the caller is told the request never
        /// happened rather than being handed an outcome no request produced.
        if (!fits(reservation, bound))
            giveUpReadDeadline("probeSentinel", key, bound, attempt - 1);

        detail::recordAttempt();
        SentinelProbeResult result{ProbeOutcome::Indeterminate, std::nullopt};
        try
        {
            result = owner.withTransportAccess(attempt, [&](auto & access)
            {
                return owner.backend->probeSentinelRaw(key, access);
            });
        }
        catch (const std::exception & e)
        {
            /// The probe never counts a fuse, so whether this fault was a credential reissue is not
            /// interesting here -- only the write and read loops guard a counter with it.
            bool refreshed = false;
            if (refreshAndClassifyReadFault(e, refresh_attempted, refreshed))
                throw;
            /// The probe reports every transport failure as `Indeterminate` rather than by throwing, so
            /// a decorator that does throw is folded onto the same inconclusive outcome.
        }

        /// The reissue is driven by the OUTCOME: this is the one primitive whose failures never reach a
        /// `catch`, and `Indeterminate` means "inconclusive", which is exactly what a retry resolves.
        /// The other four outcomes are authoritative answers and return on the first attempt.
        if (result.outcome != ProbeOutcome::Indeterminate || policy.single_attempt)
            return result;

        const uint64_t pause_ms = Retry::backoff(attempt);
        const uint64_t needed = reservedFor(pause_ms, 1);
        /// A lost fence and a spent lease budget are not things the store said, so they are reported the
        /// way every other read verb reports them rather than being dressed up as an outcome.
        switch (gate(needed))
        {
            case Gate::FenceLost: giveUpReadFenceLost("probeSentinel", key, "before the reissue");
            case Gate::NoBudget:  giveUpReadNoBudget("probeSentinel", key, "for the reissue");
            case Gate::Ok: break;
        }
        /// Only the policy's own bound ends the loop with a value: a probe DID run, and what it saw is
        /// more than a give-up exception could say. Every consumer treats `Indeterminate` fail-closed.
        if (!fits(needed, bound))
            return result;
        detail::recordReissue();
        owner.sleep_ms(pause_ms);
    }
}

std::unique_ptr<ReadBuffer> CasOperation::stream(const String & key, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    std::unique_ptr<ReadBuffer> body = readLoop("stream", key, policy, bound, [&](auto & access)
    {
        return owner.backend->stream(key, access);
    });
    if (!body)
        return nullptr;   /// absent: the open already answered
    /// By value, deliberately: nothing the buffer holds may reference this operation or its owner.
    return std::make_unique<AdmittedBodyReadBuffer>(std::move(body), key, owner.fence.admit,
                                                    admitted_generation, liveness);
}

void CasOperation::publish(const BlobPublishRequest & request, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    readLoop("publish", request.destination_key, policy, bound, [&](auto & access)
    {
        owner.backend->publish(request, access);
    });
}

CasOperation::Resolved CasOperation::observe(const String & key, const Retry & policy, const Retry::Bound & bound)
{
    last_read_stop.reset();
    try
    {
        auto got = readUnder(key, policy, bound);
        if (!got)
            return {ProvenAbsent{}, std::nullopt};
        return {std::move(*got), std::nullopt};
    }
    catch (const Exception & e)
    {
        /// A local bug replays identically on every reissue, so it is never swallowed into "nothing
        /// observed". Anything else settled nothing -- and `last_read_stop` says whether a bound
        /// refused the read or the transport itself failed, which the exception cannot.
        if (isDeterministicLocalFailure(e.code()))
            throw;
        return {NotObserved{}, last_read_stop};
    }
    catch (const std::exception & e)
    {
        /// A failure that is not the transport's is not an observation: it is the same local bug the
        /// read loop refuses to reissue, and swallowing it here would hide it just as thoroughly.
        if (!dynamic_cast<const Poco::Exception *>(&e))
            throw;
        return {NotObserved{}, last_read_stop};
    }
}

CasOperation::Resolved CasOperation::observePresence(const String & key, const Retry & policy, const Retry::Bound & bound)
{
    last_read_stop.reset();
    try
    {
        auto got = headUnder(key, policy, bound);
        if (!got)
            return {ProvenAbsent{}, std::nullopt};
        return {std::move(*got), std::nullopt};
    }
    catch (const Exception & e)
    {
        if (isDeterministicLocalFailure(e.code()))
            throw;
        return {NotObserved{}, last_read_stop};
    }
    catch (const std::exception & e)
    {
        if (!dynamic_cast<const Poco::Exception *>(&e))
            throw;
        return {NotObserved{}, last_read_stop};
    }
}

WriteResult CasOperation::gaveUp(GaveUp::Why why, GaveUp::Source source, WriteState & state) const
{
    ProfileEvents::increment(ProfileEvents::CASRequestGaveUp);
    return GaveUp{why, source, state.sent_any, state.last_seen, state.attempts_sent};
}

WriteResult CasOperation::gaveUpForReadStop(ReadStop stop, WriteState & state, const Retry::Bound & bound) const
{
    switch (stop)
    {
        case ReadStop::FenceLost:
            return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
        case ReadStop::NoBudgetLease:
            /// The fence's budget IS the mount lease, so the lease is the bound that ended this call.
            return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
        case ReadStop::PolicyExhausted:
            return gaveUp(GaveUp::Why::Deadline, sourceFor(bound), state);
    }
    UNREACHABLE();
}

WriteResult CasOperation::gaveUpAfterFailedObservation(std::optional<ReadStop> stop, WriteState & state,
                                                       const Retry::Bound & bound) const
{
    if (stop)
        return gaveUpForReadStop(*stop, state, bound);
    /// No bound refused; the read itself failed. That is what `Unresolved` names, and it is the honest
    /// answer -- claiming a deadline the clock never reached would misreport which bound to widen.
    return gaveUp(GaveUp::Why::Unresolved, sourceFor(bound), state);
}

WriteResult CasOperation::postCommit(Etag inc, bool resolved_by_read, WriteState & state, const Retry::Bound & bound)
{
    /// Admission once more, now that the write is proven durable: a fence lost here means the object
    /// may well exist, but this call must never claim it -- the caller has to resolve the key instead.
    switch (gate(0))
    {
        case Gate::FenceLost:
            ProfileEvents::increment(ProfileEvents::CASRequestFenceLostPostWrite);
            return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
        case Gate::NoBudget:
            /// The fence's budget IS the mount lease, so its refusal names the lease as the bound that
            /// ended this call, whichever bound produced the policy's own deadline.
            return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
        case Gate::Ok: break;
    }
    return Committed{std::move(inc), state.attempts_sent, resolved_by_read};
}

std::optional<WriteResult> CasOperation::gatedPause(uint64_t pause_ms, uint32_t envelopes, WriteState & state,
                                                    const Retry::Bound & bound, void (*record)(), bool should_sleep)
{
    const uint64_t needed = reservedFor(pause_ms, envelopes);
    switch (gate(needed))
    {
        case Gate::FenceLost: return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
        case Gate::NoBudget:  return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
        case Gate::Ok: break;
    }
    if (!fits(needed, bound))
        return gaveUp(GaveUp::Why::Deadline, sourceFor(bound), state);
    record();
    /// `should_sleep` is false ONLY for the fuse's zero-pause reissue: that pause is not "zero
    /// milliseconds", it is NO PAUSE AT ALL, so it must not call `sleep_ms` even with a zero argument.
    /// The three ordinary callers always sleep, even when a drawn jitter happens to be exactly zero --
    /// `Retry::backoff`'s full jitter includes zero -- so their call to `sleep_ms(0)` is kept
    /// unconditional here to leave their observable behaviour exactly as it was before this collapse.
    if (should_sleep)
        owner.sleep_ms(pause_ms);
    return std::nullopt;
}

std::optional<WriteResult> CasOperation::pauseAndReissue(WriteState & state, const Retry::Bound & bound)
{
    return gatedPause(Retry::backoff(++state.reissues), 2, state, bound, detail::recordReissue, /*should_sleep=*/true);
}

std::optional<WriteResult> CasOperation::pauseForConflict(WriteState & state, const Retry::Bound & bound)
{
    return gatedPause(Retry::conflictBackoff(), 2, state, bound, detail::recordConflictPause, /*should_sleep=*/true);
}

/// A flat pause before reissuing an attempt whose failure text named a failed connection.
static constexpr uint64_t kConnectHintPauseMs = 50;

std::optional<WriteResult> CasOperation::pauseFlat(WriteState & state, const Retry::Bound & bound)
{
    return gatedPause(kConnectHintPauseMs, 2, state, bound, detail::recordReissue, /*should_sleep=*/true);
}

std::optional<WriteResult> CasOperation::reissueAtOnce(WriteState & state, const Retry::Bound & bound)
{
    return gatedPause(0, 2, state, bound, detail::recordReissue, /*should_sleep=*/false);
}

WriteResult CasOperation::writeLoop(const String & key, const String & bytes, const std::optional<Etag> & expected,
                                    const Retry & policy, const Retry::Bound & bound, WriteState & state,
                                    ResolveWith resolve_refusal_with)
{
    std::optional<String> expected_value;
    if (expected)
        expected_value = owner.valueFor(key, *expected);

    /// This inner write's own bytes are what an ambiguity of it could have landed. A previous inner
    /// write of the same call sent DIFFERENT bytes and ended in a conflict, which proved its attempts
    /// dead -- carrying its ambiguity forward is how a competitor's identical object gets claimed.
    state.any_ambiguous = false;

    for (;;)
    {
        /// A write reserves TWO envelopes: the attempt, and the exact read that settles it. That is
        /// what keeps "every conflict is settled by one read" true at the deadline edge.
        const uint64_t reservation = reservedFor(0, 2);
        switch (gate(reservation))
        {
            case Gate::FenceLost: return gaveUp(GaveUp::Why::FenceLost, sourceFor(bound), state);
            case Gate::NoBudget:  return gaveUp(GaveUp::Why::Deadline, GaveUp::Source::Lease, state);
            case Gate::Ok: break;
        }
        if (!fits(reservation, bound))
            return gaveUp(GaveUp::Why::Deadline, sourceFor(bound), state);

        detail::recordAttempt();
        ++state.attempts_sent;
        state.sent_any = true;

        /// Disengaged means the attempt threw: its fate is unproven, and nothing may be read out of it.
        std::optional<std::expected<String, Backend::RawConflict>> outcome;
        /// A credential answer is given BEFORE the store applies anything, so the attempt provably did
        /// not land. It is the one failure that is neither a commit nor an ambiguity, and keeping it out
        /// of `any_ambiguous` is what lets a second credential failure of this inner write be refused
        /// instead of resolved by a read and reissued to the deadline.
        bool credential_answer = false;
        bool refreshed = false;
        /// Set once `refreshed` is known: true only when the credential-owned reissue below (the one
        /// guarded by this exact expression) is what will actually resend this attempt. An earlier
        /// ambiguity of this inner write routes the reissue through the ordinary hint/fuse/backoff
        /// mechanisms instead -- the credential answer never gets to skip their read or their pacing --
        /// so a hint or fuse counter must still count in that case even though credentials were refreshed.
        bool refresh_owns_reissue = false;
        bool connect_hint = false;
        bool fuse = false;
        try
        {
            outcome = owner.withTransportAccess(state.attempts_sent, [&](auto & access)
            {
                return owner.backend->write(key, bytes, expected_value, access);
            });
        }
        catch (const Exception & e)
        {
            if (isDeterministicLocalFailure(e.code()))
                throw;
            /// ONE refresh per call, only for the class a credential could explain, and only when a
            /// reissue could sign with what it installs. So an oversized entity never triggers a
            /// re-acquisition, a denial fresh credentials do not fix is refused on the second look
            /// rather than reissued to the deadline (the storage hands back a new client every time it
            /// is asked), and under a single-attempt policy the refusal below is literally "no refresh
            /// installed credentials and no earlier ambiguity".
            credential_answer = isRefreshableCredentialError(e);
            if (credential_answer && !state.refresh_attempted && !policy.single_attempt)
            {
                state.refresh_attempted = true;
                refreshed = owner.backend->refreshCredentials();
            }
            /// Mirrors the condition guarding the credential-owned reissue below exactly, so the two
            /// can never drift: `state.any_ambiguous` here is still this attempt's INCOMING value,
            /// because the update below only fires when `!credential_answer`, which `refreshed` implies
            /// false for. `!policy.single_attempt` is redundant today -- `refreshed` can only be set
            /// above under `!policy.single_attempt` already -- but it is kept so this stays an exact
            /// copy of the reissue branch's condition rather than a hand-simplified one that could
            /// silently stop matching it.
            refresh_owns_reissue = refreshed && !policy.single_attempt && !state.any_ambiguous;
            /// A refusal that FOLLOWS an ambiguous attempt of this inner write proves nothing about that
            /// attempt, so it is settled by the read below instead of ending the call here.
            const bool definitely_refused = !refreshed && isDefinitelyRefusedWrite(e);
            if (definitely_refused && !state.any_ambiguous)
            {
                ProfileEvents::increment(ProfileEvents::CASRequestRefused);
                return Refused{e.code(), e.message(), state.attempts_sent};
            }
            /// A refusal-class exception is never a hint, even when an earlier ambiguity of this inner
            /// write kept it from ending the call above: that earlier attempt's fate is what the read
            /// below must settle, and a hint reissue would skip it. Both counters below are recorded
            /// here, at classification, regardless of `policy.single_attempt` (`Retry::once` never acts
            /// on either, but the attempt's transport error still named what it named) -- except when
            /// the credential answer OWNS the reissue: a credential answer whose text happens to also
            /// match a hint or fuse text is a credential reissue, not a hint or fuse one, and must not
            /// inflate these counts. When an earlier ambiguity of this inner write keeps the credential
            /// answer from owning the reissue, the hint/fuse mechanism reissues it instead, exactly as
            /// if credentials had never been refreshed, so the counter must still count it.
            connect_hint = !definitely_refused && isConnectFailureHint(e);
            if (connect_hint && !refresh_owns_reissue)
                ProfileEvents::increment(ProfileEvents::CASRequestConnectFailureHint);
            /// Checked AFTER the hint, so a hinted attempt stays hinted (reissued before its read); a
            /// fuse timeout is reissued after the settle read runs below.
            fuse = isFirstAttemptFuseTimeout(e, state.attempts_sent);
            if (fuse && !refresh_owns_reissue)
                ProfileEvents::increment(ProfileEvents::CASRequestFirstAttemptFuse);
        }
        catch (const std::exception & e)
        {
            /// Only the transport can have landed anything, and every exception it raises is a
            /// `Poco::Exception`. A local fault -- a bad allocation, a logic error raised inside the
            /// attempt -- is not a store answer, and settling it by a read would bury the bug behind an
            /// outcome the store never gave. What is left is an unmodeled transport failure that may
            /// still have landed, so `outcome` stays disengaged and the read below settles it.
            if (!dynamic_cast<const Poco::Exception *>(&e))
                throw;
        }

        if (outcome && outcome->has_value())
        {
            if (auto inc = owner.tryMint(key, std::move(**outcome)))
                return postCommit(std::move(*inc), /*resolved_by_read=*/false, state, bound);
            /// A 2xx carrying a value no grammar accepts: the write may well have landed, so this is an
            /// ambiguity to settle by reading, never a corruption verdict about the object.
            outcome.reset();
        }
        if (!outcome && !credential_answer)
            state.any_ambiguous = true;

        /// Nothing for a read to settle: this attempt did not apply, and no EARLIER attempt of this
        /// inner write is unresolved either. Re-send it under the credentials the refresh installed.
        if (refresh_owns_reissue)
        {
            if (auto given_up = pauseAndReissue(state, bound))
                return *given_up;
            continue;
        }

        /// The failure text named a failed CONNECTION -- counted above, at classification, whether or
        /// not this policy or the deadline/fence gates below let the reissue actually happen. A read now
        /// would meet the same broken condition, so the reissue itself is the cheaper probe: the attempt
        /// stays ambiguous (`any_ambiguous` is set above), and if the reissue meets a refused precondition
        /// the read below settles it.
        if (connect_hint && !policy.single_attempt)
        {
            if (auto given_up = pauseFlat(state, bound))
                return *given_up;
            continue;
        }

        /// Every refused precondition, and every ambiguous attempt that was not reissued on a
        /// connect-failure hint, is settled by ONE exact read, under every policy: a refused
        /// precondition does not say WHO holds the key, and a 404 and a 412 reach here as the same
        /// answer. A hinted attempt reaches this read only when its reissue meets a refused
        /// precondition. A refused precondition needs only to know WHAT is there, so a presence-only
        /// caller settles it with a HEAD; proving an ambiguous attempt landed needs the bytes, and there
        /// the body read is unavoidable.
        ProfileEvents::increment(ProfileEvents::CASRequestResolveRead);
        const Resolved resolved = resolve_refusal_with == ResolveWith::Presence && !state.any_ambiguous
            ? observePresence(key, policy, bound)
            : observe(key, policy, bound);
        state.last_seen = resolved.seen;
        /// A bound refused the resolve, so say WHICH. Erasing it here is what let a lost fence be
        /// reported as an ordinary conflict and a lease refusal as a policy deadline.
        if (resolved.stop)
            return gaveUpForReadStop(*resolved.stop, state, bound);

        /// No attempt of this inner write is unresolved, so the refused precondition IS the answer.
        /// Identical bytes here are somebody else's object, and the caller that owns the key's meaning
        /// decides what that means.
        if (!state.any_ambiguous)
            return Conflict{state.last_seen, state.attempts_sent, state.any_ambiguous};

        if (!preconditionStillSatisfiable(state.last_seen, expected))
        {
            /// The precondition has MOVED, and THAT is what makes byte equality a proof: an attempt
            /// that never applied leaves the key exactly as it found it, so under an incarnation that
            /// did not move our own bytes cannot be told from the bytes already there. A create reaches
            /// here for every object it sees -- an occupied key never satisfies its precondition.
            if (const auto * obj = std::get_if<Object>(&state.last_seen); obj && obj->bytes == bytes)
                return postCommit(obj->etag, /*resolved_by_read=*/true, state, bound);
            /// Nothing of this inner write's is at the key, and a reissue would be refused too.
            return Conflict{state.last_seen, state.attempts_sent, state.any_ambiguous};
        }

        /// Unresolved but repeatable: the precondition would still be met -- or nothing was observed at
        /// all, which proves neither way and leaves this write's ambiguity alive. A reissue is what
        /// settles it, and re-sending the same bytes under the same precondition is safe: it ends at
        /// the store's own answer. A policy with no reissue has to say it settled nothing.
        if (policy.single_attempt)
            return gaveUp(GaveUp::Why::Unresolved, sourceFor(bound), state);
        /// The first attempt met the adaptive first-attempt timeout: a connection-quality answer, not a
        /// store fault. The read above settled nothing new about it, so re-send at once as attempt 2
        /// under the full attempt budget; the backoff index is untouched because no store fault was
        /// seen yet.
        if (fuse)
        {
            if (auto given_up = reissueAtOnce(state, bound))
                return *given_up;
            continue;
        }
        if (auto given_up = pauseAndReissue(state, bound))
            return *given_up;
    }
}

WriteResult CasOperation::create(const String & key, const String & bytes, const Retry & policy)
{
    WriteState state;
    return writeLoop(key, bytes, std::nullopt, policy, policy.bind(owner.now_ms()), state, ResolveWith::Body);
}

WriteResult CasOperation::replace(const String & key, const String & bytes, const Etag & seen, const Retry & policy)
{
    WriteState state;
    return writeLoop(key, bytes, seen, policy, policy.bind(owner.now_ms()), state, ResolveWith::Body);
}

WriteResult CasOperation::readModifyWrite(const String & key, const DecideOnObject & decide, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    WriteState state;

    Resolved resolved = observe(key, policy, bound);
    state.last_seen = resolved.seen;
    std::optional<Object> current;
    if (const auto * obj = std::get_if<Object>(&state.last_seen))
        current = *obj;
    else if (!std::holds_alternative<ProvenAbsent>(state.last_seen))
        return gaveUpAfterFailedObservation(resolved.stop, state, bound);

    for (;;)
    {
        /// Outside every classification: `decide` is the caller's own control flow, and an exception
        /// from it is theirs to see unchanged.
        const std::optional<String> next = decide(current);
        if (!next)
            return Declined{state.last_seen};

        WriteResult result = writeLoop(key, *next,
            current ? std::optional<Etag>(current->etag) : std::nullopt, policy, bound, state,
            ResolveWith::Body);
        if (!std::holds_alternative<Conflict>(result))
            return result;

        /// The write's own resolve read IS this iteration's read: a hot key never costs a second GET
        /// per conflict.
        if (const auto * obj = std::get_if<Object>(&state.last_seen))
            current = *obj;
        else if (std::holds_alternative<ProvenAbsent>(state.last_seen))
            current.reset();

        if (policy.single_attempt)
            return result;
        /// A clean lost race is settled: the resolve read holds the fresh object and the next
        /// iteration decides on it. Only a conflict that settled a transport fault is paced by the
        /// growing schedule.
        if (auto given_up = state.any_ambiguous ? pauseAndReissue(state, bound) : pauseForConflict(state, bound))
            return *given_up;

        /// Only when the resolve settled nothing is a fresh read owed; otherwise `current` already is
        /// what the store held.
        if (std::holds_alternative<NotObserved>(state.last_seen))
        {
            resolved = observe(key, policy, bound);
            state.last_seen = resolved.seen;
            if (const auto * obj = std::get_if<Object>(&state.last_seen))
                current = *obj;
            else if (std::holds_alternative<ProvenAbsent>(state.last_seen))
                current.reset();
            else
                return gaveUpAfterFailedObservation(resolved.stop, state, bound);
        }
    }
}

WriteResult CasOperation::readModifyWriteOnPresence(const String & key, const DecideOnMeta & decide, const Retry & policy)
{
    const Retry::Bound bound = policy.bind(owner.now_ms());
    WriteState state;

    Resolved resolved = observePresence(key, policy, bound);
    state.last_seen = resolved.seen;
    std::optional<Meta> current;
    if (const auto * meta = std::get_if<Meta>(&state.last_seen))
        current = *meta;
    else if (!std::holds_alternative<ProvenAbsent>(state.last_seen))
        return gaveUpAfterFailedObservation(resolved.stop, state, bound);

    for (;;)
    {
        const std::optional<String> next = decide(current);
        if (!next)
            return Declined{state.last_seen};

        WriteResult result = writeLoop(key, *next,
            current ? std::optional<Etag>(current->etag) : std::nullopt, policy, bound, state,
            ResolveWith::Presence);
        /// A refused precondition was settled by a HEAD, but proving an ambiguous attempt landed needs
        /// the bytes; this loop is presence-only by contract, so that body stops here.
        state.last_seen = withoutBody(std::move(state.last_seen));
        if (!std::holds_alternative<Conflict>(result))
            return withoutBody(std::move(result));

        if (const auto * meta = std::get_if<Meta>(&state.last_seen))
            current = *meta;
        else if (std::holds_alternative<ProvenAbsent>(state.last_seen))
            current.reset();

        if (policy.single_attempt)
            return Conflict{state.last_seen, state.attempts_sent, state.any_ambiguous};
        /// A clean lost race is settled: the resolve read holds the fresh object and the next
        /// iteration decides on it. Only a conflict that settled a transport fault is paced by the
        /// growing schedule.
        if (auto given_up = state.any_ambiguous ? pauseAndReissue(state, bound) : pauseForConflict(state, bound))
            return *given_up;

        if (std::holds_alternative<NotObserved>(state.last_seen))
        {
            resolved = observePresence(key, policy, bound);
            state.last_seen = resolved.seen;
            if (const auto * meta = std::get_if<Meta>(&state.last_seen))
                current = *meta;
            else if (std::holds_alternative<ProvenAbsent>(state.last_seen))
                current.reset();
            else
                return gaveUpAfterFailedObservation(resolved.stop, state, bound);
        }
    }
}

}
