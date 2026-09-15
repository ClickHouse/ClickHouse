#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasTransportAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasWriteResult.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasFence.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasThrottlingBackend.h>
#include "cas_test_helpers.h"
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>

#include <IO/ReadHelpers.h>
#include <IO/S3/Client.h>
#include <IO/WriteBufferFromS3.h>

#include "config.h"

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketImpl.h>
#include <base/defines.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <gmock/gmock.h>

#include <fmt/format.h>

#include <atomic>
#include <cerrno>
#include <functional>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
extern const int CAS_DELETE_MARKER;
extern const int CORRUPTED_DATA;
extern const int LOGICAL_ERROR;
extern const int S3_ERROR;
extern const int NETWORK_ERROR;
}

namespace ProfileEvents
{
    extern const Event CASRequestReissue;
    extern const Event CASRequestConflictPause;
    extern const Event CASRequestConnectFailureHint;
    extern const Event CASRequestFirstAttemptFuse;
}

using namespace DB::Cas;

using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::FakeClock;
using DB::Cas::tests::expectBytes;
using DB::Cas::tests::expectThrowsCode;

namespace
{

/// Every engine test drives `CasRequests` on an injected clock, so a ninety-second policy is exercised
/// in no wall-clock time and the retry schedule itself becomes an assertion.
CasRequests makeRequests(BackendPtr backend, FakeClock & clock, Fence fence = Fence::open())
{
    return CasRequests(std::move(backend), std::move(fence), clock.nowFn(), clock.sleepFn());
}

}

static_assert(!std::is_default_constructible_v<Etag>);
static_assert(!std::is_constructible_v<Etag, String>);
static_assert(!std::is_constructible_v<Etag, PersistedEtag>);
static_assert(!std::is_default_constructible_v<TransportAccess>);
static_assert(!std::is_copy_constructible_v<TransportAccess>);

TEST(CASIncarnation, GrammarRefusesTheNineWays)
{
    EXPECT_FALSE(isIncarnationValue(Dialect::ETag, ""));
    EXPECT_FALSE(isIncarnationValue(Dialect::ETag, "*"));
    EXPECT_FALSE(isIncarnationValue(Dialect::ETag, " * "));
    EXPECT_FALSE(isIncarnationValue(Dialect::ETag, "\"a\",\"b\""));
    EXPECT_TRUE(isIncarnationValue(Dialect::ETag, "\"abc\""));
    EXPECT_FALSE(isIncarnationValue(Dialect::Generation, "0"));
    EXPECT_FALSE(isIncarnationValue(Dialect::Generation, "00123"));
    EXPECT_FALSE(isIncarnationValue(Dialect::Generation, "\"123\""));
    EXPECT_FALSE(isIncarnationValue(Dialect::Generation, "123 "));   /// the ninth: decimal is not "decimal, trimmed"
    EXPECT_TRUE(isIncarnationValue(Dialect::Generation, "123"));
    EXPECT_FALSE(isIncarnationValue(Dialect::Emulated, ""));
}

TEST(CASRetry, BackoffIsFullJitterUnderTheCap)
{
    for (uint32_t attempt = 1; attempt <= 12; ++attempt)
    {
        const uint64_t ceiling = std::min<uint64_t>(5000, 200ull << (attempt - 1));
        uint64_t sum = 0;
        std::set<uint64_t> seen;
        bool low = false;
        bool high = false;
        for (int i = 0; i < 1000; ++i)
        {
            const uint64_t s = Retry::backoff(attempt);
            ASSERT_LE(s, ceiling);
            sum += s;
            seen.insert(s);
            low = low || s < ceiling / 4;
            high = high || s > ceiling * 3 / 4;
        }
        const double mean = static_cast<double>(sum) / 1000.0;
        EXPECT_GT(mean, static_cast<double>(ceiling) * 0.35) << "attempt " << attempt;
        EXPECT_LT(mean, static_cast<double>(ceiling) * 0.65) << "attempt " << attempt;
        /// The mean alone cannot tell full jitter from a constant half the ceiling, so the SPREAD is
        /// asserted too: many distinct values, reaching into both the bottom and the top quarter.
        EXPECT_GE(seen.size(), 3u) << "attempt " << attempt;
        EXPECT_TRUE(low) << "attempt " << attempt;
        EXPECT_TRUE(high) << "attempt " << attempt;
    }
}

TEST(CASRetry, PoliciesAreShapedAsSpecified)
{
    const uint64_t now = 1'000'000;
    EXPECT_EQ(Retry::standard().bind(now).deadline_ms, now + 90'000);
    EXPECT_FALSE(Retry::standard().bind(now).lease_bound);
    EXPECT_FALSE(Retry::standard().single_attempt);
    EXPECT_TRUE(Retry::once().single_attempt);
    const Retry::Bound lease = Retry::untilLeaseSafe(now + 10'000, 2'000).bind(now);
    EXPECT_EQ(lease.deadline_ms, now + 8'000);
    EXPECT_TRUE(lease.lease_bound);
    EXPECT_EQ(Retry::within(1'000).bind(now).deadline_ms, now + 1'000);
}

/// A frozen policy is ONE absolute deadline: time passing does not buy a later one, freezing again
/// cannot extend it, and the lease bound still wins when it is the smaller of the two -- which is what
/// keeps `GaveUp::Source` able to say which bound refused.
TEST(CASRetry, AFrozenPolicyIsOneDeadlineAndTheLeaseStillWins)
{
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    const uint64_t start = clock.now;
    const Retry frozen = op.freeze(Retry::standard());
    ASSERT_TRUE(frozen.policy_deadline_ms.has_value());
    EXPECT_EQ(frozen.bind(start).deadline_ms, start + 90'000);
    EXPECT_EQ(frozen.bind(start + 50'000).deadline_ms, start + 90'000);
    EXPECT_FALSE(frozen.bind(start + 50'000).lease_bound);
    /// The single-attempt view of a frozen policy keeps the deadline rather than starting a window.
    EXPECT_EQ(frozen.asSingleAttempt().policy_deadline_ms, frozen.policy_deadline_ms);
    EXPECT_TRUE(frozen.asSingleAttempt().single_attempt);

    clock.now += 50'000;
    EXPECT_EQ(op.freeze(frozen).policy_deadline_ms, frozen.policy_deadline_ms);

    const Retry::Bound leashed = op.freeze(Retry::untilLeaseSafe(start + 10'000, 2'000)).bind(clock.now);
    EXPECT_EQ(leashed.deadline_ms, start + 8'000);
    EXPECT_TRUE(leashed.lease_bound);
}

/// Freezing belongs to a loop. A single verb still gets a full window from where it is called, however
/// long its caller has already been running.
TEST(CASRequests, ALoneReadUnderTheStandardPolicyStillGetsItsFullWindow)
{
    FakeClock clock;
    auto throttled = std::make_shared<ThrottlingBackend>(
        std::make_shared<InMemoryBackend>(), ThrottlingBackend::Mode::EveryNth, 1, 429);
    auto requests = makeRequests(throttled, clock);
    auto op = requests.admit();

    clock.now += 10 * 90'000;
    const uint64_t start = clock.now;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_GE(clock.now - start, 85'000u);
}

TEST(CASWriteResult, OrThrowMapsEveryAlternative)
{
    /// The two that are not failures: a commit hands back its incarnation, a decline hands back
    /// nothing, and neither throws. Minting one needs a real write, since nothing else may mint.
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult committed = op.create("k", "v", Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(committed));
    const Etag landed = std::get<Committed>(committed).etag;
    const auto returned = orThrow(std::move(committed), "create");
    ASSERT_TRUE(returned.has_value());
    EXPECT_EQ(*returned, landed);
    EXPECT_FALSE(orThrow(WriteResult{Declined{ProvenAbsent{}}}, "declined").has_value());

    expectThrowsCode(DB::ErrorCodes::ABORTED, [&] { orThrow(WriteResult{Conflict{ProvenAbsent{}}}, "t"); });
    expectThrowsCode(DB::ErrorCodes::S3_ERROR, [&] { orThrow(WriteResult{Refused{DB::ErrorCodes::S3_ERROR, "denied"}}, "t"); });
    /// Designated rather than positional: `GaveUp` grows fields at its end, and a positional list is
    /// the form a field inserted anywhere else would silently re-interpret.
    const GaveUp deadline{
        .why = GaveUp::Why::Deadline, .deadline_source = GaveUp::Source::Policy,
        .sent_any = true, .last_seen = NotObserved{}};
    const GaveUp unresolved{
        .why = GaveUp::Why::Unresolved, .deadline_source = GaveUp::Source::Policy,
        .sent_any = true, .last_seen = ProvenAbsent{}};
    const GaveUp fence_lost{
        .why = GaveUp::Why::FenceLost, .deadline_source = GaveUp::Source::Lease,
        .sent_any = false, .last_seen = NotObserved{}};
    for (const GaveUp & gave_up : {deadline, unresolved, fence_lost})
        expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { orThrow(WriteResult{gave_up}, "t"); });
}

TEST(CASFence, OpenFenceAdmitsEverythingAndNeverMoves)
{
    Fence f = Fence::open();
    EXPECT_EQ(f.generation(), 0u);
    EXPECT_EQ(f.admit(0, 1'000'000), Fence::Admit::Ok);
    EXPECT_NO_THROW(f.check_or_throw(0));
}

/// ================================================================================================
/// The backend's keyed string primitives
/// ================================================================================================

TEST(CASBackendPrimitives, InMemoryWriteReadRemoveRoundTripThroughOneOperation)
{
    FakeClock clock;
    auto b = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(b, clock);
    auto op = requests.admit();

    const std::optional<Etag> w1 = orThrow(op.create("k", "v1", Retry::once()), "create");
    ASSERT_TRUE(w1);
    const std::optional<Object> r = op.read("k", Retry::once());
    ASSERT_TRUE(r);
    EXPECT_EQ(r->bytes, "v1");
    EXPECT_EQ(r->etag, *w1);

    const std::optional<Meta> h = op.head("k", Retry::once());
    ASSERT_TRUE(h);
    EXPECT_EQ(h->size, 2u);
    EXPECT_EQ(h->etag, *w1);

    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("k", "v2", Retry::once())));   /// must be absent
    const std::optional<Etag> w3 = orThrow(op.replace("k", "v2", *w1, Retry::once()), "replace");
    ASSERT_TRUE(w3);
    EXPECT_NE(*w3, *w1);                                  /// incarnations never repeat

    EXPECT_EQ(op.remove("k", *w1, Retry::once()), Removal::Mismatch);
    EXPECT_EQ(op.remove("k", *w3, Retry::once()), Removal::Removed);
    EXPECT_EQ(op.remove("k", *w3, Retry::once()), Removal::Gone);
    EXPECT_FALSE(op.read("k", Retry::once()).has_value());
}

TEST(CASBackendPrimitives, ListSurfacesTheIncarnationAndPaginates)
{
    FakeClock clock;
    auto b = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(b, clock);
    auto op = requests.admit();

    const std::optional<Etag> a = orThrow(op.create("p/a", "0123456789", Retry::once()), "create");
    ASSERT_TRUE(a);
    orThrow(op.create("p/b", "xy", Retry::once()), "create");
    orThrow(op.create("q/c", "z", Retry::once()), "create");

    const ListPage page = op.list("p/", "", 10, Retry::once());
    ASSERT_EQ(page.keys.size(), 2u);                      /// sorted, prefix-scoped
    EXPECT_EQ(page.keys[0].key, "p/a");
    EXPECT_EQ(page.keys[0].size, 10u);
    ASSERT_TRUE(page.keys[0].etag.has_value());
    EXPECT_EQ(*page.keys[0].etag, *a);
    EXPECT_TRUE(page.next_cursor.empty());

    const ListPage first = op.list("p/", "", 1, Retry::once());
    ASSERT_EQ(first.keys.size(), 1u);
    EXPECT_EQ(first.next_cursor, "p/a");
    const ListPage second = op.list("p/", first.next_cursor, 1, Retry::once());
    ASSERT_EQ(second.keys.size(), 1u);
    EXPECT_EQ(second.keys[0].key, "p/b");
}

TEST(CASBackendPrimitives, EveryBackendInstanceHasItsOwnId)
{
    auto a = std::make_shared<InMemoryBackend>();
    auto b = std::make_shared<InMemoryBackend>();
    EXPECT_NE(a->backendId(), b->backendId());
    EXPECT_NE(a->backendId(), 0u);
    EXPECT_EQ(a->dialect(), Dialect::Emulated);
}

/// The legacy verbs (`putIfAbsent`/`casPut`/`putOverwrite`) that used to forward through the primitive
/// `write` are gone -- `CasOperation` is the only caller of `Backend` now -- so that forwarding is a
/// type-level guarantee rather than a runtime check. What remains to prove is that every fault double
/// in this file that overrides `write` sees an ATTEMPT under either shape `CasOperation` can send:
/// unconditional (`create`) and Etag-conditioned (`replace`).
/// `EachWriteKnobIsKeyedAndOneShotOnThePrimitiveWrite` below covers both.

TEST(CASBackendPrimitives, EachWriteKnobIsKeyedAndOneShotOnThePrimitiveWrite)
{
    /// A knob names a KEY, not a call site: the keyed `write` every write reaches, whichever
    /// `CasOperation` verb (`create`/`replace`) issued it.
    auto b = std::make_shared<InMemoryBackend>();
    FakeClock clock;
    auto requests = makeRequests(b, clock);
    auto op = requests.admit();

    b->refuseNextWrite("k");
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("k", "v", Retry::once())));   /// consumed here
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::once())));  /// and only once
    expectBytes(b, "k", "v");

    b->refuseNextWrite("k2");
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("k2", "v", Retry::once())));
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k2", "v", Retry::once())));

    b->injectAmbiguousWrite("k3");
    EXPECT_TRUE(std::holds_alternative<GaveUp>(op.create("k3", "v", Retry::once())));
    EXPECT_FALSE(op.read("k3", Retry::once()).has_value()) << "an ambiguous write leaves the store untouched";
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k3", "v", Retry::once())));

    /// Both knobs on one key, each consumed by the next write in turn.
    b->injectAmbiguousWrite("k4");
    b->refuseNextWrite("k4");
    EXPECT_TRUE(std::holds_alternative<GaveUp>(op.create("k4", "v", Retry::once())));
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.create("k4", "v", Retry::once())));
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k4", "v", Retry::once())));

    /// The Etag-conditioned shape: every write above was unconditional (`create`), so none of them
    /// could have caught a fault double that only intercepts `write` when it carries an
    /// `expected_value` -- the shape `replace` alone sends.
    const std::optional<Etag> k5_first = orThrow(op.create("k5", "v", Retry::once()), "create");
    ASSERT_TRUE(k5_first);
    b->refuseNextWrite("k5");
    EXPECT_TRUE(std::holds_alternative<Conflict>(op.replace("k5", "v2", *k5_first, Retry::once())))
        << "consumed here";
    const std::optional<Etag> k5_second
        = orThrow(op.replace("k5", "v2", *k5_first, Retry::once()), "replace");   /// and only once
    ASSERT_TRUE(k5_second);
    expectBytes(b, "k5", "v2");
}

TEST(CASBackendPrimitives, ReadRefusesAValueThatIsNotAnIncarnation)
{
    /// `read` hands back whatever the store said, malformed included -- `CasRequests::mint` is what
    /// refuses it, naming the key, before any caller can see it as an `Etag`.
    struct EmptyValueBackend : InMemoryBackend
    {
        std::optional<Raw> read(const String &, TransportAccess &) override { return Raw{"body", ""}; }
    };
    auto b = std::make_shared<EmptyValueBackend>();
    FakeClock clock;
    auto requests = makeRequests(b, clock);
    auto op = requests.admit();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { op.read("k", Retry::once()); });
}

/// `InstrumentedBackendPassesALegacyCallThroughAsLegacy` pinned `InstrumentedBackend` delegating the
/// legacy `casPut` verb to its inner backend unconverted. `Backend` has no legacy verbs left --
/// `InstrumentedBackend` is a pure primitive decorator now -- and its primitive delegation (`write` and
/// every other primitive, classified and counted) is what `CASInstrumentedBackend.ClassifierAndPerNamespaceOpEvents`
/// (gtest_cas_backend.cpp) pins.

TEST(CASBackendPrimitives, RefreshCredentialsIsOffUntilAskedFor)
{
    auto b = std::make_shared<InMemoryBackend>();
    EXPECT_FALSE(b->refreshCredentials());
    b->setRefreshCredentialsResult(true);
    EXPECT_TRUE(b->refreshCredentials());
}

#if USE_AWS_S3

TEST(CASThrottlingBackend, FirstPerKeyRefusesOnceAndTheCallStillSucceeds)
{
    FakeClock clock;
    auto inner = std::make_shared<InMemoryBackend>();
    auto t = std::make_shared<ThrottlingBackend>(inner, ThrottlingBackend::Mode::FirstPerKey, 0, 429);
    auto requests = makeRequests(t, clock);
    auto op = requests.admit();

    orThrow(op.create("k2", "v", Retry::standard()), "create");
    EXPECT_EQ(t->refusals("k2"), 1u);
    EXPECT_TRUE(op.read("k2", Retry::standard()).has_value());
    EXPECT_EQ(t->refusals("k2"), 1u) << "only the FIRST request naming a key is refused";
}

TEST(CASThrottlingBackend, RefusalsAreRetryableUnderBothStatuses)
{
    /// The property the seam exists for: a refusal must reach the engine as an AMBIGUOUS attempt, not
    /// a definite failure. What proves it is that the engine REISSUES -- a definite failure would
    /// surface unchanged, with the refusal still the only request the store ever saw.
    for (const int status : {429, 503})
    {
        FakeClock clock;
        auto t = std::make_shared<ThrottlingBackend>(
            std::make_shared<InMemoryBackend>(), ThrottlingBackend::Mode::FirstPerKey, 0, status);
        auto requests = makeRequests(t, clock);
        auto op = requests.admit();

        EXPECT_FALSE(op.head("k", Retry::standard()).has_value()) << "status " << status;
        EXPECT_EQ(t->refusals("k"), 1u) << "status " << status;
    }
}

/// `PassesALegacyCallThroughAsLegacy` pinned `ThrottlingBackend` delegating the legacy `casPut` verb
/// unconverted. `Backend` has no legacy verbs left; `ThrottlingBackend`'s primitive pass-through is
/// pinned by `FirstPerKeyRefusesOnceAndTheCallStillSucceeds` above and `EveryNthRefusesOnThePeriodAcrossKeys`
/// below, both of which drive it through `CasOperation`.

TEST(CASThrottlingBackend, EveryNthRefusesOnThePeriodAcrossKeys)
{
    FakeClock clock;
    auto inner = std::make_shared<InMemoryBackend>();
    auto t = std::make_shared<ThrottlingBackend>(inner, ThrottlingBackend::Mode::EveryNth, 3, 503);
    auto requests = makeRequests(t, clock);
    auto op = requests.admit();

    EXPECT_FALSE(op.read("a", Retry::standard()).has_value());
    EXPECT_FALSE(op.read("b", Retry::standard()).has_value());
    /// The THIRD request is refused whatever it names; the engine reissues it as the fourth.
    EXPECT_FALSE(op.read("c", Retry::standard()).has_value());
    EXPECT_EQ(t->refusals("c"), 1u);
    EXPECT_EQ(t->refusals("a"), 0u);
    EXPECT_EQ(t->refusals("b"), 0u);
}

#endif

/// ================================================================================================
/// The request engine
/// ================================================================================================

namespace
{

/// A type nothing in the engine catches, so a `decide` that throws it can only reach the caller by
/// propagating unchanged.
struct DecideMarker
{
};

/// Answers the FIRST remove with a mismatch without reaching the store, so `removeCurrent` has to
/// re-observe. Counts its own requests: an answer given here never reaches the counting base.
struct MismatchOnceOnRemoveBackend : InMemoryBackend
{
    using InMemoryBackend::head;

    size_t heads = 0;
    size_t removes = 0;
    bool refuse_next_remove = true;

    std::optional<RawMeta> head(const String & key, TransportAccess & access) override
    {
        ++heads;
        return InMemoryBackend::head(key, access);
    }

    RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        ++removes;
        if (std::exchange(refuse_next_remove, false))
            return RawRemoval::Mismatch;
        return InMemoryBackend::remove(key, expected_value, access);
    }
};

/// Answers `Indeterminate` for its first `indeterminate_answers` probes, then delegates -- a store
/// briefly out of reach, whose absence was never established.
struct IndeterminateProbeBackend : InMemoryBackend
{
    using Backend::probeSentinelRaw;

    size_t probes = 0;
    size_t indeterminate_answers = 2;

    SentinelProbeResult probeSentinelRaw(const String & key, TransportAccess & access) override
    {
        if (++probes <= indeterminate_answers)
            return {ProbeOutcome::Indeterminate, std::nullopt};
        return InMemoryBackend::probeSentinelRaw(key, access);
    }
};

/// Refuses the FIRST `list` naming each distinct cursor -- one refusal per page -- and charges every
/// list a fixed slice of the caller's clock, so what a page costs is a fact rather than a jitter draw.
/// `always_refuse_cursor` keeps one page refused for good.
struct PagedThrottleBackend : InMemoryBackend
{
    using InMemoryBackend::list;

    std::function<void()> charge_latency;
    std::set<String> refused_cursors;
    std::optional<String> always_refuse_cursor;
    size_t list_calls = 0;

    RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
    {
        ++list_calls;
        if (charge_latency)
            charge_latency();
        if ((always_refuse_cursor && *always_refuse_cursor == cursor) || refused_cursors.insert(cursor).second)
            throw Poco::TimeoutException("the list resuming after '" + cursor + "' timed out");
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }
};

/// Runs `on_read` after every read. The resolve read is where a caller's own facts can change
/// between an attempt and the pause that would precede the next one.
struct FlipOnReadBackend : CountingBackend
{
    std::function<void()> on_read;

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        auto raw = CountingBackend::read(key, access);
        if (on_read)
            on_read();
        return raw;
    }
};

}

TEST(CASIncarnation, RenderAndPersistedCompare)
{
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    const Etag first = *orThrow(op.create("k", "v", Retry::standard()), "create");
    EXPECT_EQ(first.render(), "emulated:1");
    EXPECT_EQ(first.key(), "k");
    EXPECT_EQ(first.dialect(), Dialect::Emulated);

    const PersistedEtag persisted = PersistedEtag::capture(first);
    EXPECT_EQ(persisted.dialect, "emulated");
    EXPECT_EQ(persisted.value, "1");
    EXPECT_TRUE(persisted.matches(first));

    const Etag second = *orThrow(op.replace("k", "w", first, Retry::standard()), "replace");
    EXPECT_EQ(second.render(), "emulated:2");
    EXPECT_FALSE(persisted.matches(second));   /// a captured record never re-matches a later incarnation
    EXPECT_TRUE(PersistedEtag::capture(second).matches(second));
}

TEST(CASRetry, BindSaturatesAndLeavesAnEqualLeaseOffTheLeaseSource)
{
    constexpr uint64_t largest = std::numeric_limits<uint64_t>::max();
    /// A window one short of the whole range, so any `now` above 1 overflows a naive addition.
    EXPECT_EQ(Retry::within(largest - 1).bind(2).deadline_ms, largest);
    EXPECT_EQ(Retry::within(largest - 1).bind(1).deadline_ms, largest);
    EXPECT_FALSE(Retry::within(largest - 1).bind(2).lease_bound);

    const uint64_t now = 1'000'000;
    /// The lease bound lands exactly on the policy deadline. The lease is taken only when it is
    /// STRICTLY smaller, so the tie belongs to the policy and `GaveUp` will not name the lease.
    const Retry::Bound tie = Retry::untilLeaseSafe(now + 92'000, 2'000).bind(now);
    EXPECT_EQ(tie.deadline_ms, now + 90'000);
    EXPECT_FALSE(tie.lease_bound);

    const Retry::Bound lease = Retry::untilLeaseSafe(now + 91'999, 2'000).bind(now);
    EXPECT_EQ(lease.deadline_ms, now + 89'999);
    EXPECT_TRUE(lease.lease_bound);
}

TEST(CASRequests, CreateThenReplaceThenRemove)
{
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    const Etag first = *orThrow(op.create("k", "v1", Retry::standard()), "create");
    const auto seen = op.read("k", Retry::standard());
    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(seen->bytes, "v1");
    EXPECT_EQ(seen->etag, first);

    const Etag second = *orThrow(op.replace("k", "v2", first, Retry::standard()), "replace");
    EXPECT_NE(second, first);

    EXPECT_EQ(op.remove("k", first, Retry::standard()), Removal::Mismatch);    /// the incarnation is stale
    EXPECT_EQ(op.remove("k", second, Retry::standard()), Removal::Removed);
    EXPECT_EQ(op.remove("k", second, Retry::standard()), Removal::Gone);
    EXPECT_FALSE(op.read("k", Retry::standard()).has_value());
}

/// An incarnation observed for one key is refused as the precondition for another, before the write
/// loop starts anything. Constructing a `LOGICAL_ERROR` exception ABORTS under a debug or sanitizer
/// build, so the same contract is asserted there as a death expectation; both forms pin that the
/// refusal happens, and the non-death form additionally pins that it costs no request.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRequests, KeyBindingThrowsBeforeAnyRequest)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag of_a = *orThrow(op.create("a", "v", Retry::standard()), "create");
    backend->resetCounts();

    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { (void)op.replace("b", "w", of_a, Retry::standard()); });
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}
#else
TEST(CASRequestsDeathTest, KeyBindingThrowsBeforeAnyRequest)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag of_a = *orThrow(op.create("a", "v", Retry::standard()), "create");

    EXPECT_DEATH({ (void)op.replace("b", "w", of_a, Retry::standard()); }, "");
}
#endif

TEST(CASRequests, EveryConflictIsSettledByOneReadAndCarriesTheOccupant)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "theirs", Retry::standard()), "create");
    backend->resetCounts();

    WriteResult result = op.create("k", "mine", Retry::once());
    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_NE(conflict, nullptr);
    const auto * occupant = std::get_if<Object>(&conflict->seen);
    ASSERT_NE(occupant, nullptr);
    EXPECT_EQ(occupant->bytes, "theirs");

    /// The refused precondition says only that the key is taken; ONE exact read says by whom.
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
}

TEST(CASRequests, AmbiguousCreateThatLandedIsCommittedByTheResolveRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    /// The object becomes durable and THEN the response is lost, so the store holds bytes the caller
    /// never learned it wrote -- the only ambiguity a resolve read can settle as a commit.
    backend->injectAmbiguousLandedWrite("k");
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_TRUE(committed->resolved_by_read);
    EXPECT_EQ(committed->attempts_sent, 1u);
    /// Settled by reading, never by writing again: a second create would have conflicted with the
    /// first one's own object.
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, AmbiguousCreateThatNeverLandedIsReissued)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousWrite("k");   /// the attempt's outcome is lost and the store is untouched
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(committed->attempts_sent, 2u);
    EXPECT_EQ(backend->getTotal(), 1u);     /// the resolve proved absence, and only then did a reissue follow
    EXPECT_EQ(clock.sleeps.size(), 1u);
}

/// The engine's own attempt number reaches the transport through `TransportAccess::attemptNo()`, for
/// every primitive -- write, read (the resolve read is its own call, with its own attempt count) and
/// list.
TEST(CASRequests, TheTransportSeesTheEngineAttemptNumber)
{
    struct AttemptRecordingBackend : CountingBackend
    {
        std::vector<size_t> write_attempts, read_attempts, list_attempts;
        std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                                 const std::optional<String> & expected, TransportAccess & access) override
        {
            write_attempts.push_back(access.attemptNo());
            return CountingBackend::write(key, bytes, expected, access);
        }
        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            read_attempts.push_back(access.attemptNo());
            return CountingBackend::read(key, access);
        }
        RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
        {
            list_attempts.push_back(access.attemptNo());
            return CountingBackend::list(prefix, cursor, limit, access);
        }
    };
    FakeClock clock;
    auto backend = std::make_shared<AttemptRecordingBackend>();
    backend->injectAmbiguousWrite("k");
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("read timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::standard())));
    /// Attempt 1 ambiguous, attempt 2 commits. The settle read is its OWN read call: attempt 1 failed, 2 answered.
    EXPECT_EQ(backend->write_attempts, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(backend->read_attempts, (std::vector<size_t>{1, 2}));
    backend->list_attempts.clear();
    (void)op.list("p/", "", 10, Retry::standard());
    EXPECT_EQ(backend->list_attempts, (std::vector<size_t>{1}));
}

TEST(CASRequests, OnceSendsOneWriteAndAtMostOneResolveRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("the read timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    /// One attempt is one attempt, but the read that would have settled it is still owed and sent.
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, DecideMayThrowAndTheExceptionPropagatesUnchanged)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    EXPECT_THROW(
        op.readModifyWrite("k", [](const std::optional<Object> &) -> std::optional<String> { throw DecideMarker{}; },
                           Retry::standard()),
        DecideMarker);
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 1u);   /// the key was read, and nothing was decided about it
}

TEST(CASRequests, OnPresenceIssuesHeadsAndNoGet)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> & current) -> std::optional<String>
        {
            return current ? std::nullopt : std::optional<String>("v");
        },
        Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    EXPECT_EQ(backend->getTotal(), 0u);
    /// One HEAD decided it and one write landed it: the loop issues no request it does not need.
    EXPECT_EQ(backend->headTotal(), 1u);
    EXPECT_EQ(backend->writeTotal(), 1u);
}

TEST(CASRequests, OnPresenceSettlesARefusedPreconditionWithAHead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->refuseNextWrite("k");   /// the store refuses the precondition, writing nothing
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> & current) -> std::optional<String>
        {
            return current ? std::nullopt : std::optional<String>("v");
        },
        Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    /// A refused precondition needs only to know WHAT is at the key, so this loop never fetches a body.
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->headTotal(), 2u);
    EXPECT_EQ(backend->writeTotal(), 2u);
}

TEST(CASRequests, ForEachListedKeyStopsEarlyAndBudgetsPerPage)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    for (int i = 0; i < 25; ++i)
        orThrow(op.create("p/" + std::to_string(i), "v", Retry::standard()), "create");
    backend->resetCounts();

    size_t seen = 0;
    size_t pages = 0;
    op.forEachListedKey("p/", [&](const ListedKey &) { return ++seen < 3; }, Retry::standard(),
                        /*page_limit=*/10, [&] { ++pages; });
    EXPECT_EQ(seen, 3u);
    /// The walk stops where the caller stops it: the remaining two pages are never fetched.
    EXPECT_EQ(pages, 1u);
    EXPECT_EQ(backend->listTotal(), 1u);
}

TEST(CASRequests, DeleteMarkerIsANamedException)
{
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag inc = *orThrow(op.create("k", "v", Retry::standard()), "create");

    backend->setSimulateDeleteMarkers(true);
    expectThrowsCode(DB::ErrorCodes::CAS_DELETE_MARKER, [&] { (void)op.remove("k", inc, Retry::standard()); });
    EXPECT_TRUE(clock.sleeps.empty());   /// a versioned bucket answers this way every time
}

TEST(CASRequests, RemoveCurrentReObservesAMismatchAndRefusesUnderOnce)
{
    {
        FakeClock clock;
        auto backend = std::make_shared<MismatchOnceOnRemoveBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "create");

        EXPECT_EQ(op.removeCurrent("k", Retry::standard()), Removal::Removed);
        /// Another incarnation became current between the observation and the delete: observe again,
        /// paced like every other reissue, and delete what the second look saw.
        EXPECT_EQ(backend->heads, 2u);
        EXPECT_EQ(backend->removes, 2u);
        EXPECT_EQ(clock.sleeps.size(), 1u);
    }
    {
        FakeClock clock;
        auto backend = std::make_shared<MismatchOnceOnRemoveBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "create");

        /// `once` has no reissue with which to settle a mismatch, and this verb never hands one back.
        expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)op.removeCurrent("k", Retry::once()); });
        EXPECT_EQ(backend->heads, 1u);
        EXPECT_EQ(backend->removes, 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
}

TEST(CASRequests, ProbeSentinelRetriesOnlyTheIndeterminateOutcome)
{
    {
        FakeClock clock;
        auto backend = std::make_shared<IndeterminateProbeBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "create");

        const SentinelProbeResult result = op.probeSentinel("k", Retry::standard());
        EXPECT_EQ(result.outcome, ProbeOutcome::Present);
        ASSERT_TRUE(result.body.has_value());
        EXPECT_EQ(*result.body, "v");
        EXPECT_EQ(backend->probes, 3u);          /// inconclusive twice, then an authoritative answer
        EXPECT_EQ(clock.sleeps.size(), 2u);
    }
    {
        FakeClock clock;
        auto backend = std::make_shared<IndeterminateProbeBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "create");

        /// With no reissue left, the inconclusive outcome IS the answer: reported, never thrown.
        const SentinelProbeResult result = op.probeSentinel("k", Retry::once());
        EXPECT_EQ(result.outcome, ProbeOutcome::Indeterminate);
        EXPECT_EQ(backend->probes, 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
}

TEST(CASRequests, AdmissionIsCheckedAtThreePoints)
{
    FakeClock clock;
    auto backend = std::make_shared<InMemoryBackend>();
    uint64_t generation = 1;
    bool lost = false;
    Fence fence{
        [&] { return generation; },
        [&](uint64_t admitted, uint64_t)
        {
            return (lost || admitted != generation) ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok;
        },
        [&](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    /// The store is observed through an OPEN fence: these checks run while the subject's own fence is
    /// closed, and a fenced read would report the fence rather than the store.
    auto observer_requests = makeRequests(backend, clock);
    auto observer = observer_requests.admit();

    /// (1) before the first attempt, on a handle resumed under a generation the fence has moved past
    {
        auto op = requests.resume(0);
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_FALSE(gave_up->sent_any);
        EXPECT_FALSE(observer.read("k", Retry::once()).has_value());
    }
    /// (2) before the next verb of an admitted handle, after a re-arm between two verbs
    {
        auto op = requests.admit();
        EXPECT_FALSE(op.head("k", Retry::standard()).has_value());
        generation = 2;
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_FALSE(gave_up->sent_any);
        EXPECT_FALSE(observer.read("k", Retry::once()).has_value());
    }
    /// (3) after a proven commit: the write landed, then the fence tripped before the call returned
    {
        auto op = requests.admit();
        backend->onWriteCommitted("k2", [&] { lost = true; });
        WriteResult result = op.create("k2", "v", Retry::standard());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_TRUE(gave_up->sent_any);
        /// The object IS durable. This call refuses to CLAIM it; it does not undo it.
        EXPECT_TRUE(observer.read("k2", Retry::once()).has_value());
    }
}

TEST(CASRequests, TheGateBeforeTheSleepEndsTheCallWithoutASecondWrite)
{
    FakeClock clock;
    auto backend = std::make_shared<FlipOnReadBackend>();
    bool alive = true;
    backend->on_read = [&] { alive = false; };
    backend->injectAmbiguousWrite("k");
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit([&] { return alive; });

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_TRUE(gave_up->sent_any);
    /// The ambiguous attempt was resolved, and the pause before the reissue was refused rather than
    /// served: no sleep, and no second attempt after it.
    EXPECT_TRUE(clock.sleeps.empty());
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
}

TEST(CASRequests, AResolveReadRefusedForLeaseBudgetIsReportedAsTheLeaseDeadline)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    bool lease_spent = false;
    Fence fence{
        [] { return uint64_t{0}; },
        [&](uint64_t, uint64_t) { return lease_spent ? Fence::Admit::NoBudget : Fence::Admit::Ok; },
        [](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    auto op = requests.admit();
    const Etag seen = *orThrow(op.create("k", "v", Retry::standard()), "create");

    /// The store refuses the precondition, and the lease budget is gone by the time the read that
    /// would say WHO holds the key is due. The call learned nothing about the key, so what it reports
    /// is the bound that stopped it -- not a conflict it never observed.
    backend->refuseNextWrite("k");
    backend->onBeforeWrite("k", [&] { lease_spent = true; });
    const uint64_t lease_deadline = clock.now + 10'000;
    WriteResult result = op.replace("k", "w", seen, Retry::untilLeaseSafe(lease_deadline, 2'000));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Lease);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    EXPECT_TRUE(clock.sleeps.empty());
    EXPECT_EQ(backend->writeTotal(), 2u);   /// the create and the one refused replace
    EXPECT_EQ(backend->getTotal(), 0u);    /// the resolve read never started
}

TEST(CASRequests, AFenceWithNoBudgetForTheRequestSendsNothingAndNamesTheLease)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    const uint64_t budget_ms = 500;
    Fence fence{
        [] { return uint64_t{0}; },
        [&](uint64_t, uint64_t needed_ms) { return needed_ms > budget_ms ? Fence::Admit::NoBudget : Fence::Admit::Ok; },
        [](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    /// One attempt reserves more than the lease has left, so nothing may be started under it.
    requests.setAttemptReservationForTest(1'000);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    /// The policy's own window is untouched; what ran out is the fence's budget, which IS the lease.
    EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Lease);
    EXPECT_FALSE(gave_up->sent_any);

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, AnRmwWhoseFirstReadFailsGivesUpUnresolvedWithoutWriting)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("the read timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.readModifyWrite("k",
        [](const std::optional<Object> &) -> std::optional<String> { return String("v"); }, Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    /// No BOUND refused this read; the read itself failed. Claiming a deadline the clock never reached
    /// would send its reader to widen the wrong thing.
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    EXPECT_EQ(backend->writeTotal(), 0u);
}

TEST(CASRequests, AnOnPresenceRmwWhoseFirstHeadFailsGivesUpUnresolvedWithoutWriting)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextHeadWith("k", std::make_exception_ptr(Poco::TimeoutException("the head timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> &) -> std::optional<String> { return String("v"); }, Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);   /// the presence loop does not fall back to a body read
}

TEST(CASRequests, AConflictWhoseResolveReadFailsIsReportedWithNothingObserved)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "theirs", Retry::standard()), "create");

    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("the read timed out")));
    WriteResult result = op.create("k", "mine", Retry::once());
    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_NE(conflict, nullptr);
    /// The precondition was refused, so the key IS taken; the read that would have said by whom failed,
    /// and the caller is told exactly that rather than handed a guess about the occupant.
    EXPECT_TRUE(std::holds_alternative<NotObserved>(conflict->seen));
}

TEST(CASRequests, AFenceLostDuringTheResolveReadIsAFenceLossNotAConflict)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    bool alive = true;
    /// The fence trips while the ambiguous attempt is in flight: the write's own hook runs before the
    /// store is touched, so the resolve read is the first request to meet the closed gate.
    backend->onBeforeWrite("k", [&] { alive = false; });
    backend->injectAmbiguousWrite("k");
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit([&] { return alive; });

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    /// A lost fence is not an observation. Reporting it as an ordinary conflict would tell the caller
    /// somebody else holds the key, when what happened is that this node stopped being allowed to ask.
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 0u);   /// refused before the resolve read was issued
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, OnPresenceFetchesTheBodyToProveAnAmbiguousAttemptLanded)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousLandedWrite("k");
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> & current) -> std::optional<String>
        {
            return current ? std::nullopt : std::optional<String>("v");
        },
        Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_TRUE(committed->resolved_by_read);
    /// Presence-only is what this loop REPORTS, not a promise about what it may read: only the bytes
    /// can prove the ambiguous attempt was this call's own.
    EXPECT_EQ(backend->getTotal(), 1u);
}

TEST(CASRequests, OnPresenceReportsMetaEvenWhenItHadToFetchTheBody)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto competitor = makeRequests(backend, clock);
    auto rival = competitor.admit();

    /// A competitor takes the key while our own create is in flight, and that create's own fate is
    /// lost. The ambiguity is armed from inside the hook so the competitor's write cannot consume it.
    bool staged = false;
    std::optional<Etag> rival_etag;
    backend->onBeforeWrite("k", [&]
    {
        if (staged)
            return;
        staged = true;
        const WriteResult rival_result = rival.create("k", "theirs", Retry::once());
        const auto * rival_committed = std::get_if<Committed>(&rival_result);
        ASSERT_NE(rival_committed, nullptr);
        rival_etag = rival_committed->etag;
        backend->injectAmbiguousWrite("k");
    });

    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> &) -> std::optional<String> { return String("mine"); }, Retry::once());
    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_NE(conflict, nullptr);
    /// The ambiguity forced a body read, and the body stops at this boundary: a caller of the
    /// presence loop can never come to depend on bytes the loop does not promise. `get_if<Meta>` plus
    /// its field checks, not a bare `holds_alternative`: a variant that already proved it holds `Meta`
    /// cannot also hold `Object`, so the field checks are what a regression could actually fail --
    /// proving the observed Meta is the RIVAL's own committed incarnation, not some other object.
    ASSERT_TRUE(rival_etag.has_value());
    const auto * meta_seen = std::get_if<Meta>(&conflict->seen);
    ASSERT_NE(meta_seen, nullptr);
    EXPECT_EQ(meta_seen->etag, *rival_etag);
    EXPECT_EQ(meta_seen->size, String("theirs").size());
    EXPECT_EQ(backend->getTotal(), 1u);
}

TEST(CASRequests, AmbiguousReplaceWhoseResolveShowsThePreconditionUnchangedIsReissued)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
    backend->resetCounts();

    /// The attempt's fate is lost and the store is untouched. The incarnation it named is still the
    /// current one -- which proves nothing landed, and leaves a precondition a reissue can still meet.
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    WriteResult result = op.replace("k", "v2", seen, Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 2u);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(backend->writeTotal(), 2u);
    EXPECT_EQ(backend->getTotal(), 1u);   /// exactly one resolve read, and it settled the ambiguity
    EXPECT_EQ(clock.sleeps.size(), 1u);
}

TEST(CASRequests, AmbiguousReplaceOfIdenticalBytesIsReissuedNotClaimedByByteEquality)
{
    /// The key already holds exactly the bytes we are about to write, so byte equality alone can never
    /// say whether the ambiguous attempt landed. The incarnation can: an attempt that applied would
    /// have moved it. Under a policy with a reissue that means re-sending; under `once` it means saying
    /// the write is unresolved rather than claiming somebody else's identical object.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "B", Retry::standard()), "create");
        backend->resetCounts();

        backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
        WriteResult result = op.replace("k", "B", seen, Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        /// Claiming the resolve read's object would have reported one attempt and a commit this call
        /// never made; the reissue is what actually put these bytes there under a new incarnation.
        EXPECT_EQ(committed->attempts_sent, 2u);
        EXPECT_FALSE(committed->resolved_by_read);
        EXPECT_NE(committed->etag, seen);
        EXPECT_EQ(backend->writeTotal(), 2u);
        EXPECT_EQ(backend->getTotal(), 1u);
        EXPECT_EQ(clock.sleeps.size(), 1u);
    }
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "B", Retry::standard()), "create");
        backend->resetCounts();

        backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
        WriteResult result = op.replace("k", "B", seen, Retry::once());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
        EXPECT_TRUE(gave_up->sent_any);
        EXPECT_EQ(backend->writeTotal(), 1u);
        EXPECT_EQ(backend->getTotal(), 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
}

TEST(CASRequests, AmbiguousReplaceWhoseResolveShowsAnotherIncarnationIsAConflict)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag stale = *orThrow(op.create("k", "v1", Retry::standard()), "create");
    orThrow(op.replace("k", "theirs", stale, Retry::standard()), "the competitor's replace");
    backend->resetCounts();

    /// The attempt's fate is lost, and the key has moved past the incarnation it named: no reissue of
    /// it could ever apply, so the ambiguity is settled and the occupant is the answer.
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    WriteResult result = op.replace("k", "mine", stale, Retry::standard());
    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_NE(conflict, nullptr);
    const auto * occupant = std::get_if<Object>(&conflict->seen);
    ASSERT_NE(occupant, nullptr);
    EXPECT_EQ(occupant->bytes, "theirs");
    EXPECT_NE(occupant->etag, stale);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    /// The count the conflict reports is the count the transport saw, not a constant that happens to
    /// match here: a caller totalling attempts across endings has to be able to add this one.
    EXPECT_EQ(conflict->attempts_sent, backend->writeTotal());
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ReadModifyWriteDoesNotClaimACompetitorsIdenticalBytesAfterAnEarlierAmbiguity)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto competitor = makeRequests(backend, clock);
    auto rival = competitor.admit();

    /// The competitor moves the key once before each of our two attempts, and its own writes re-enter
    /// this hook. The ambiguity is armed here rather than up front so the competitor's create cannot
    /// consume the arming meant for ours.
    bool inside = false;
    int staged = 0;
    backend->onBeforeWrite("k", [&]
    {
        if (inside)
            return;
        inside = true;
        if (staged == 0)
        {
            (void)rival.create("k", "X", Retry::once());
            backend->injectAmbiguousWrite("k");
        }
        else if (staged == 1)
        {
            /// The bytes we are about to send, under an incarnation that is not ours.
            if (const auto current = rival.read("k", Retry::once()))
                (void)rival.replace("k", "B", current->etag, Retry::once());
        }
        ++staged;
        inside = false;
    });

    std::vector<String> decided_on;
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.readModifyWrite("k",
        [&](const std::optional<Object> & current) -> std::optional<String>
        {
            decided_on.push_back(current ? current->bytes : String("<absent>"));
            if (!current)
                return String("A");
            if (current->bytes == "X")
                return String("B");
            return std::nullopt;
        },
        Retry::standard());

    /// The only ambiguity this call had belonged to "A", and the competitor's "X" already proved it
    /// dead. "B" at the key is the competitor's, so the loop re-decides on it instead of claiming it.
    const auto * declined = std::get_if<Declined>(&result);
    ASSERT_NE(declined, nullptr);
    const auto * seen = std::get_if<Object>(&declined->seen);
    ASSERT_NE(seen, nullptr);
    EXPECT_EQ(seen->bytes, "B");
    EXPECT_EQ(decided_on, (std::vector<String>{"<absent>", "X", "B"}));
}

TEST(CASRequests, AnUnmodeledLocalExceptionOnAWritePropagatesUnchanged)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    /// Not a `Poco::Exception`, so it did not come from the transport and cannot have landed anything.
    /// Settling it by a read would report a store answer the store never gave.
    backend->failNextWriteWith("k", std::make_exception_ptr(std::logic_error("a local bug")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    EXPECT_THROW((void)op.create("k", "v", Retry::standard()), std::logic_error);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ForEachListedKeyGivesEachPageItsOwnPolicyWindow)
{
    FakeClock clock;
    auto backend = std::make_shared<PagedThrottleBackend>();
    /// Every list costs the caller 300ms, so a page's cost is a fact and not a jitter draw.
    backend->charge_latency = [&clock] { clock.now += 300; };
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    for (int i = 0; i < 25; ++i)
        orThrow(op.create("p/" + std::to_string(i), "v", Retry::standard()), "create");

    size_t seen = 0;
    size_t pages = 0;
    const uint64_t start = clock.now;
    /// A window that comfortably covers ONE page's refusal and its reissue, and could not have covered
    /// the walk: the policy governs each page, because a walk is an unbounded number of requests.
    op.forEachListedKey("p/", [&](const ListedKey &) { ++seen; return true; }, Retry::within(1'000),
                        /*page_limit=*/10, [&] { ++pages; });
    EXPECT_EQ(seen, 25u);
    EXPECT_EQ(pages, 3u);
    EXPECT_EQ(backend->list_calls, 6u);        /// each page refused once, then delivered
    EXPECT_GT(clock.now - start, 1'000u);      /// the walk outlived the window every page was given
}

TEST(CASRequests, ForEachListedKeyThrowsRatherThanTruncateWhenAPageNeverArrives)
{
    FakeClock clock;
    auto backend = std::make_shared<PagedThrottleBackend>();
    backend->charge_latency = [&clock] { clock.now += 300; };
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    for (int i = 0; i < 25; ++i)
        orThrow(op.create("p/" + std::to_string(i), "v", Retry::standard()), "create");

    const ListPage first = op.list("p/", "", 10, Retry::within(1'000));
    ASSERT_FALSE(first.next_cursor.empty());
    backend->always_refuse_cursor = first.next_cursor;   /// the second page never arrives
    backend->refused_cursors.clear();

    size_t seen = 0;
    size_t pages = 0;
    /// A silently truncated enumeration is the error a coverage record exists to prevent, so the walk
    /// reports the page it could not fetch instead of returning what it managed to read.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        op.forEachListedKey("p/", [&](const ListedKey &) { ++seen; return true; }, Retry::within(1'000),
                            /*page_limit=*/10, [&] { ++pages; });
    });
    EXPECT_EQ(pages, 1u);
    EXPECT_EQ(seen, 10u);
}

TEST(CASRequests, LivenessPredicateEndsTheOperationLikeAFenceLoss)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    bool alive = true;
    auto op = requests.admit([&] { return alive; });
    EXPECT_TRUE(op.admitted());

    alive = false;
    EXPECT_FALSE(op.admitted());

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 0u);

    /// The read surface reports the same refusal the only way it can: by exception.
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_EQ(backend->getTotal(), 0u);
}

TEST(CASRequests, ReadModifyWriteLosesNoIncrementUnderContentionAndBoundsAHotKey)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const auto increment = [](const std::optional<Object> & current) -> std::optional<String>
    {
        return std::to_string(std::stoi(current ? current->bytes : "0") + 1);
    };

    /// The real clock and the real sleep: two threads share this engine, and a `FakeClock` would be a
    /// data race on both of its fields.
    CasRequests contended(backend, Fence::open());
    {
        auto seed = contended.admit();
        orThrow(seed.create("ctr", "0", Retry::standard()), "create");
    }
    const auto fifty_increments = [&]
    {
        auto op = contended.admit();
        for (int i = 0; i < 50; ++i)
            orThrow(op.readModifyWrite("ctr", increment, Retry::standard()), "increment");
    };
    std::thread first(fifty_increments);
    std::thread second(fifty_increments);
    first.join();
    second.join();

    auto reader = contended.admit();
    const auto counted = reader.read("ctr", Retry::standard());
    ASSERT_TRUE(counted.has_value());
    EXPECT_EQ(counted->bytes, "100");   /// every conflict re-decided against what the resolve read saw

    /// A key rewritten under EVERY attempt is bounded by the deadline instead of looping forever.
    FakeClock clock;
    auto hot = makeRequests(backend, clock);
    auto competitor = makeRequests(backend, clock);
    auto rival = competitor.admit();
    bool inside_hook = false;
    backend->onBeforeWrite("ctr", [&]
    {
        if (inside_hook)   /// the hook's own write re-enters this callback
            return;
        inside_hook = true;
        if (const auto current = rival.read("ctr", Retry::once()))
            (void)rival.replace("ctr", "999", current->etag, Retry::once());
        inside_hook = false;
    });

    auto op = hot.admit();
    WriteResult result = op.readModifyWrite("ctr", increment, Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_FALSE(clock.sleeps.empty());   /// it paced its retries rather than spinning
}

namespace
{

/// Moves `key` under the caller before each of its first `moves` write attempts, and optionally makes
/// each of those attempts ambiguous (the store never answers it) so the resolve read is what settles
/// the race.
struct RaceMaker
{
    RaceMaker(std::shared_ptr<CountingBackend> backend_, FakeClock & clock, String key_, int moves_, bool ambiguous_)
        : backend(std::move(backend_)), key(std::move(key_)), moves(moves_), ambiguous(ambiguous_)
        , rival_requests(makeRequests(backend, clock)), rival(rival_requests.admit())
    {
        backend->onBeforeWrite(key, [this]
        {
            if (inside || made >= moves)
                return;
            inside = true;
            if (const auto current = rival.read(key, Retry::once()))
                (void)rival.replace(key, current->bytes + "r", current->etag, Retry::once());
            else
                (void)rival.create(key, "r", Retry::once());
            if (ambiguous)
                backend->injectAmbiguousWrite(key);
            ++made;
            inside = false;
        });
    }

    std::shared_ptr<CountingBackend> backend;
    String key;
    int moves;
    bool ambiguous;
    int made = 0;
    bool inside = false;
    CasRequests rival_requests;
    CasOperation rival;
};

DecideOnObject appendX()
{
    return [](const std::optional<Object> & current) -> std::optional<String>
    {
        return current ? current->bytes + "x" : String("x");
    };
}

}

TEST(CASRequests, CleanConflictsArePacedFlatAndDoNotAdvanceTheReissueCounter)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    (void)orThrow(op.create("k", "v", Retry::standard()), "seed");
    constexpr int K = 4;
    RaceMaker races(backend, clock, "k", K, /*ambiguous=*/false);
    const auto pauses_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConflictPause].load();
    const auto reissues_before = ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load();

    WriteResult result = op.readModifyWrite("k", appendX(), Retry::standard());

    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConflictPause].load() - pauses_before, K);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load() - reissues_before, 0u);
    ASSERT_EQ(clock.sleeps.size(), static_cast<size_t>(K));
    for (uint64_t s : clock.sleeps)
        EXPECT_LE(s, 200u);   /// flat: every pause is one `backoff(1)` draw, whatever the loss count
    EXPECT_EQ(backend->writeCount("k"), 1u + K + 1u + K);   /// seed, K refused, K rival moves, the one that landed
}

TEST(CASRequests, AConflictThatSettledAFaultKeepsTheGrowingSchedule)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    (void)orThrow(op.create("k", "v", Retry::standard()), "seed");
    constexpr int K = 3;
    RaceMaker races(backend, clock, "k", K, /*ambiguous=*/true);
    const auto pauses_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConflictPause].load();
    const auto reissues_before = ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load();

    WriteResult result = op.readModifyWrite("k", appendX(), Retry::standard());

    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConflictPause].load() - pauses_before, 0u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load() - reissues_before, K);
    ASSERT_EQ(clock.sleeps.size(), static_cast<size_t>(K));
    for (size_t i = 0; i < clock.sleeps.size(); ++i)
        EXPECT_LE(clock.sleeps[i], std::min<uint64_t>(5000, 200ull << i)) << "reissue " << i;
}

TEST(CASRequests, ReplaceReportsWhetherAConflictSettledAFault)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const Etag seed = *orThrow(op.create("k", "v", Retry::standard()), "seed");
    {
        RaceMaker clean(backend, clock, "k", 1, /*ambiguous=*/false);
        WriteResult result = op.replace("k", "w", seed, Retry::standard());
        const auto * conflict = std::get_if<Conflict>(&result);
        ASSERT_NE(conflict, nullptr);
        EXPECT_FALSE(conflict->any_ambiguous);
        EXPECT_EQ(conflict->attempts_sent, 1u);
    }
    {
        RaceMaker faulty(backend, clock, "k", 1, /*ambiguous=*/true);
        WriteResult result = op.replace("k", "w", seed, Retry::standard());
        const auto * conflict = std::get_if<Conflict>(&result);
        ASSERT_NE(conflict, nullptr);
        EXPECT_TRUE(conflict->any_ambiguous);
        EXPECT_EQ(conflict->attempts_sent, 1u);   /// one attempt, lost, settled as moved: `attempts_sent` cannot tell
    }
}

TEST(CASRequests, OnPresenceUnderOnceKeepsTheFaultFlagOnTheRebuiltConflict)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    (void)orThrow(op.create("k", "v", Retry::standard()), "seed");
    RaceMaker faulty(backend, clock, "k", 1, /*ambiguous=*/true);

    WriteResult result = op.readModifyWriteOnPresence("k",
        [](const std::optional<Meta> &) -> std::optional<String> { return String("w"); }, Retry::once());

    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_NE(conflict, nullptr);
    EXPECT_TRUE(conflict->any_ambiguous);
    EXPECT_TRUE(std::holds_alternative<Meta>(conflict->seen));   /// presence-only, as before
}

TEST(CASRequests, CleanConflictsBeforeAFaultDoNotInflateTheFaultsFirstBackoff)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    (void)orThrow(op.create("k", "v", Retry::standard()), "seed");
    constexpr int K = 3;
    RaceMaker races(backend, clock, "k", K, /*ambiguous=*/false);
    /// After the K clean races the next attempt is ambiguous with the precondition unchanged, so the
    /// engine reissues it; that reissue's pause must be the schedule's first, not its (K+1)-th.
    bool armed = false;
    backend->onBeforeWrite("k", [&]
    {
        /// The RaceMaker's hook is replaced by this one; it moves the key itself for the first K writes.
        if (races.inside)
            return;
        if (races.made < K)
        {
            races.inside = true;
            if (const auto current = races.rival.read("k", Retry::once()))
                (void)races.rival.replace("k", current->bytes + "r", current->etag, Retry::once());
            ++races.made;
            races.inside = false;
            return;
        }
        if (!armed)
        {
            armed = true;
            backend->injectAmbiguousWrite("k");
        }
    });

    WriteResult result = op.readModifyWrite("k", appendX(), Retry::standard());

    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    ASSERT_EQ(clock.sleeps.size(), static_cast<size_t>(K + 1));
    EXPECT_LE(clock.sleeps[K], 200u) << "the first transport reissue sleeps within backoff(1)";
}

TEST(CASRequests, ADeterministicLocalFailureSurfacesUnchangedWithoutAReissue)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextReadWith("k", std::make_exception_ptr(
        DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "the object at 'k' is not decodable")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    /// Reissuing would replay the same bug and bury it behind a retryable exception at the deadline.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ATransportTimeoutIsReissuedAndALocalFailureIsNot)
{
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "create");
        backend->resetCounts();

        backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("the read timed out")));
        const auto seen = op.read("k", Retry::standard());
        ASSERT_TRUE(seen.has_value());
        EXPECT_EQ(seen->bytes, "v");
        EXPECT_EQ(backend->getTotal(), 2u);
        EXPECT_EQ(clock.sleeps.size(), 1u);
    }
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        /// Not a `Poco::Exception`, so it did not come from the transport: reissuing it would spend the
        /// whole deadline replaying a local bug.
        backend->failNextReadWith("k", std::make_exception_ptr(std::logic_error("a local bug")));
        EXPECT_THROW((void)op.read("k", Retry::standard()), std::logic_error);
        EXPECT_EQ(backend->getTotal(), 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
}

#if USE_AWS_S3

namespace
{

/// An `S3Exception` carrying a canonical `<Code>` name. The name is how the request contract tells
/// one store answer from another: the SDK reports every error it does not model as `UNKNOWN`, so the
/// code alone can never stand for a particular failure.
std::exception_ptr s3Error(Aws::S3::S3Errors code, const String & name)
{
    return std::make_exception_ptr(DB::S3Exception("the store answered " + name, code, name));
}

/// Answers EVERY read with the same store error. A classification that terminates on an error is then
/// visible as a single attempt, and one that keeps the error ambiguous as a policy spent to its
/// deadline -- which a one-shot arming could never tell apart.
class AlwaysFailingReadBackend final : public CountingBackend
{
public:
    explicit AlwaysFailingReadBackend(std::exception_ptr error_) : error(std::move(error_)) {}

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        (void)CountingBackend::read(key, access);
        std::rethrow_exception(error);
    }

private:
    std::exception_ptr error;
};

}

TEST(CASRequests, DeadlineIsTheOnlyBoundUnderZeroLatencyThrottling)
{
    FakeClock clock;
    auto throttled = std::make_shared<ThrottlingBackend>(
        std::make_shared<InMemoryBackend>(), ThrottlingBackend::Mode::EveryNth, 1, 429);
    auto requests = makeRequests(throttled, clock);
    auto op = requests.admit();

    const uint64_t start = clock.now;
    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Policy);
    EXPECT_TRUE(gave_up->sent_any);
    /// What ends the call is the policy's own deadline, not a count of attempts: it kept issuing to
    /// within one backoff of that deadline, and paused many more times than a small fixed budget allows.
    EXPECT_GE(clock.now - start, 85'000u);
    EXPECT_GT(clock.sleeps.size(), 16u);
}

TEST(CASRequests, LeaseBoundPolicyIssuesNothingPastTheBoundary)
{
    FakeClock clock;
    auto throttled = std::make_shared<ThrottlingBackend>(
        std::make_shared<InMemoryBackend>(), ThrottlingBackend::Mode::EveryNth, 1, 429);
    auto requests = makeRequests(throttled, clock);
    requests.setAttemptReservationForTest(1'000);

    const uint64_t lease_deadline = clock.now + 10'000;
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::untilLeaseSafe(lease_deadline, 2'000));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    /// The lease was the smaller of the two bounds, and the give-up names it rather than the policy.
    EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Lease);
    /// Nothing is STARTED that could not finish inside the bound: the last request began at least one
    /// attempt reservation before lease minus margin.
    EXPECT_LE(clock.now, lease_deadline - 2'000 - 1'000);
}

TEST(CASRequests, AMalformedRequestIsRefusedWithoutAReissue)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::UNKNOWN, "MalformedXML"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * refused = std::get_if<Refused>(&result);
    ASSERT_NE(refused, nullptr);
    EXPECT_EQ(refused->store_error, DB::ErrorCodes::S3_ERROR);
    /// The store's own answer proves the request never applied: nothing to resolve, nothing to reissue,
    /// and no credential the refusal could be about.
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, AnAccessDenialNoRefreshCanFixIsRefusedOnTheFirstAttempt)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(false);
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Refused>(result));
    /// A refresh is asked for once and installs nothing, and THAT is what makes the denial terminal.
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ASecondCredentialAnswerAfterTheOneRefreshIsRefused)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    /// The store answers a denial BEFORE it applies anything, so neither attempt landed and no read
    /// has anything to settle. A call gets one refresh, so the denial that survives it is the answer.
    const auto * refused = std::get_if<Refused>(&result);
    ASSERT_NE(refused, nullptr);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_EQ(backend->writeTotal(), 2u);
    EXPECT_EQ(backend->getTotal(), 0u);
    /// BOTH attempts are counted, not just the one that produced the answer -- which is why this is
    /// asserted on the refusal that took two rather than on one of the single-attempt refusals.
    EXPECT_EQ(refused->attempts_sent, backend->writeTotal());
    EXPECT_EQ(clock.sleeps.size(), 1u);   /// the one paced re-send under the credentials it installed
}

TEST(CASRequests, UnderOnceACredentialAnswerIsRefusedWithoutARefresh)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    /// A refresh that WOULD have installed credentials, so the zero below is the gate and not the
    /// storage refusing to hand any back.
    backend->setRefreshCredentialsResult(true);
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    /// Fresh credentials only help a reissue, and `once` has none to sign, so none are asked for --
    /// which is what keeps `Refused` meaning "no refresh installed credentials and no earlier
    /// ambiguity" rather than "a refresh helped and the answer stood anyway".
    WriteResult result = op.create("k", "v", Retry::once());
    ASSERT_TRUE(std::holds_alternative<Refused>(result));
    EXPECT_EQ(backend->refreshCredentialsCalls(), 0u);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ACredentialAnswerAfterAnAmbiguousAttemptStillOwesTheResolveRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    /// The first attempt's fate is unknown and it may yet land; the second is a proven non-application.
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    /// The refresh does not license a direct re-send here: the OTHER attempt is still unresolved, so
    /// the read that settles it is still owed.
    EXPECT_EQ(backend->getTotal(), 2u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
}

TEST(CASRequests, AnExpiredTokenARefreshFixesIsResentWithoutAResolveRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID, "ExpiredToken"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 2u);
    EXPECT_FALSE(committed->resolved_by_read);
    /// The credential answer proves its OWN attempt never applied, and no earlier attempt of this call
    /// is unresolved, so the re-send under the fresh credentials owes no read.
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_EQ(clock.sleeps.size(), 1u);
}

TEST(CASRequests, AnExpiredTokenNoRefreshCanFixIsRefusedRatherThanRiddenToTheDeadline)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", s3Error(Aws::S3::S3Errors::INVALID_CLIENT_TOKEN_ID, "ExpiredToken"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    WriteResult result = op.create("k", "v", Retry::standard());
    /// The refusal class CONTAINS the refresh class: an expired credential that no refresh installed
    /// would otherwise spend the whole deadline being reissued.
    ASSERT_TRUE(std::holds_alternative<Refused>(result));
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ANameOnlyAccessDenialOnAReadPropagatesWhenNoRefreshIsAvailable)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(false);
    /// Matched by NAME alone: the SDK reports this store's denial under its catch-all code, so the
    /// name is the only thing that says a credential could explain it.
    backend->failNextReadWith("k", s3Error(Aws::S3::S3Errors::UNKNOWN, "AccessDenied"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    /// One refresh is asked for and installs nothing, so nothing would sign differently: the read
    /// propagates instead of spending its policy on a request that cannot start succeeding.
    expectThrowsCode(DB::ErrorCodes::S3_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, ReadModifyWriteWhoseResolveAndFreshObservationBothFailGivesUpUnresolved)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "v0", Retry::standard()), "create");
    backend->resetCounts();

    /// The store refuses the precondition, and both reads that would settle what happened answer with
    /// a store refusal the read loop surfaces at once rather than reissuing. They are armed from inside
    /// the write so the loop's OWN first read still succeeds and `decide` sees the object.
    backend->refuseNextWrite("k");
    bool armed = false;
    backend->onBeforeWrite("k", [&]
    {
        if (armed)
            return;
        armed = true;
        backend->failNextReadWith("k", s3Error(Aws::S3::S3Errors::UNKNOWN, "MalformedXML"));
        backend->failNextReadWith("k", s3Error(Aws::S3::S3Errors::UNKNOWN, "MalformedXML"));
    });

    WriteResult result = op.readModifyWrite("k",
        [](const std::optional<Object> &) -> std::optional<String> { return String("v1"); }, Retry::standard());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    /// No BOUND refused either read -- the reads themselves failed -- so naming a deadline the clock
    /// never reached would send its reader to widen the wrong thing, and nothing was observed to
    /// report as a conflict.
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_TRUE(std::holds_alternative<NotObserved>(gave_up->last_seen));
    /// The write count is unchanged after the first attempt: nothing ever said another one was safe.
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 3u);
    EXPECT_EQ(clock.sleeps.size(), 1u);
}

/// A missing bucket is an ANSWER the store gave, but not an answer about the object: an S3-compatible
/// store that transiently misroutes a bucket says exactly this, and a read that ended on it would turn
/// an availability blip into a hard failure. It stays in the ambiguous class -- reissued until the
/// policy's deadline -- like a throttle or a 5xx.
TEST(CASRequests, AMissingBucketOnAReadIsReissuedToTheDeadline)
{
    FakeClock clock;
    auto backend = std::make_shared<AlwaysFailingReadBackend>(
        s3Error(Aws::S3::S3Errors::NO_SUCH_BUCKET, "NoSuchBucket"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    const uint64_t start = clock.now;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_GT(backend->getTotal(), 1u) << "the read ended on its first attempt instead of reissuing";
    EXPECT_GE(clock.now - start, 85'000u) << "the policy's own deadline is what must end this read";
}

/// The kept half of the same classification: a key miss IS an answer about the object, so reissuing it
/// only replays the same authoritative absence until the deadline. One attempt, no pause.
TEST(CASRequests, AnAuthoritativeKeyMissOnAReadEndsTheCallAtOnce)
{
    FakeClock clock;
    auto backend = std::make_shared<AlwaysFailingReadBackend>(
        s3Error(Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey"));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    expectThrowsCode(DB::ErrorCodes::S3_ERROR, [&] { (void)op.read("k", Retry::standard()); });
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
}

TEST(CASRequests, AnUnmodeledStoreErrorOnAReadIsReissuedNotSurfaced)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "v", Retry::standard()), "create");
    backend->resetCounts();

    /// An S3-compatible store's own vendor code. The SDK models it as `UNKNOWN`, which is its code for
    /// EVERY error it does not know, so it can never stand for "this will not start succeeding".
    backend->failNextReadWith("k", s3Error(Aws::S3::S3Errors::UNKNOWN, "SomeVendorCode"));
    const auto seen = op.read("k", Retry::standard());
    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(seen->bytes, "v");
    EXPECT_EQ(backend->getTotal(), 2u);
    EXPECT_EQ(clock.sleeps.size(), 1u);
}

#endif

/// A write reserves TWO request envelopes, not one: the attempt, and the read that settles it if the
/// attempt comes back ambiguous. At exactly one reservation of surplus before lease minus margin there
/// is room for the attempt alone, and an engine that reserved only the attempt would start one it
/// could not settle inside the bound. Nothing may be sent.
TEST(CASRequests, AWriteReservesTwoEnvelopesSoOneOfSurplusStartsNothing)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    requests.setAttemptReservationForTest(1'000);

    const uint64_t lease_deadline = clock.now + 2'000 + 1'000;
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::untilLeaseSafe(lease_deadline, 2'000));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_EQ(gave_up->deadline_source, GaveUp::Source::Lease);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_TRUE(clock.sleeps.empty());
}


/// The body of a streamed object is read at the consumer's pace, long after the opening attempt
/// returned; the wrapper re-admits it at every refill. The window the open already loaded is served
/// first -- the SDK buffer arrives with pending data -- and the check first fires on advancing past it.
TEST(CASRequests, StreamBodyKeepsThePreloadedWindowAndRefusesOnTheNextRefill)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    std::atomic<bool> torn_down{false};
    Fence fence{[] { return uint64_t{0}; },
                [&](uint64_t, uint64_t) { return torn_down.load() ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
                [](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    auto op = requests.admit();
    orThrow(op.create("k", "0123456789", Retry::once()), "create");
    backend->setStreamChunkForTest(4);   /// the body arrives as "0123", "4567", "89"

    auto body = op.stream("k", Retry::once());
    ASSERT_TRUE(body);
    String first(4, '\0');
    body->readStrict(first.data(), 4);
    EXPECT_EQ(first, "0123") << "the window the open already loaded is served, not skipped";

    torn_down.store(true);
    char c;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { body->readStrict(&c, 1); });
    EXPECT_TRUE(body->isCanceled()) << "a refused refill leaves the buffer the consumer holds unusable";
}

TEST(CASRequests, StreamBodyServesEveryWindowThenEof)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "0123456789", Retry::once()), "create");
    backend->setStreamChunkForTest(4);

    auto body = op.stream("k", Retry::once());
    ASSERT_TRUE(body);
    String all;
    DB::readStringUntilEOF(all, *body);
    EXPECT_EQ(all, "0123456789");
    EXPECT_TRUE(body->eof());
    EXPECT_FALSE(op.stream("absent", Retry::once())) << "an absent object is still the open's answer";
}

/// The mount plane's fence can answer `NoBudget`; a body refused for that reason must read like a
/// refused open on the same plane -- the retry-later class, not a tripped fence.
TEST(CASRequests, StreamBodyRefusalKeepsTheNoBudgetMapping)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    std::atomic<bool> out_of_budget{false};
    Fence fence{[] { return uint64_t{0}; },
                [&](uint64_t, uint64_t) { return out_of_budget.load() ? Fence::Admit::NoBudget : Fence::Admit::Ok; },
                [](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    auto op = requests.admit();
    orThrow(op.create("k", "0123456789", Retry::once()), "create");
    backend->setStreamChunkForTest(4);

    auto body = op.stream("k", Retry::once());
    ASSERT_TRUE(body);
    String first(4, '\0');
    body->readStrict(first.data(), 4);
    out_of_budget.store(true);
    char c;
    try
    {
        body->readStrict(&c, 1);
        FAIL() << "the refill must be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_NE(e.message().find("no lease budget"), String::npos) << e.message();
    }
}

/// The caller's liveness is the second half of admission for the body too, in the gate's order.
TEST(CASRequests, StreamBodyHonoursTheCallersLiveness)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    std::atomic<bool> alive{true};
    auto op = requests.admit([&] { return alive.load(); });
    orThrow(op.create("k", "0123456789", Retry::once()), "create");
    backend->setStreamChunkForTest(4);

    auto body = op.stream("k", Retry::once());
    ASSERT_TRUE(body);
    String first(4, '\0');
    body->readStrict(first.data(), 4);
    alive.store(false);
    char c;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { body->readStrict(&c, 1); });
}

/// The window the open already loaded is served WITHOUT a further admission: `Backend::stream` forces
/// that first GET and accounts it to the open's own attempt, so re-checking it here would refuse
/// bytes the caller has already paid for. The refusal belongs to the SECOND window, the first one the
/// body actually asks the store for. Armed before the first read, so a wrapper that discarded the
/// adopted window would refuse immediately instead of serving it.
TEST(CASRequests, StreamBodyServesTheAdoptedWindowEvenWhenAdmissionIsAlreadyRefused)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    std::atomic<bool> torn_down{false};
    Fence fence{[] { return uint64_t{0}; },
                [&](uint64_t, uint64_t) { return torn_down.load() ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
                [](uint64_t) {}};
    auto requests = makeRequests(backend, clock, fence);
    auto op = requests.admit();
    orThrow(op.create("k", "0123456789", Retry::once()), "create");
    backend->setStreamChunkForTest(4);

    auto body = op.stream("k", Retry::once());
    ASSERT_TRUE(body);
    torn_down.store(true);   /// refused BEFORE the consumer touches the body

    String first(4, '\0');
    body->readStrict(first.data(), 4);
    EXPECT_EQ(first, "0123") << "the window the open already paid for must be served, not re-admitted";
    EXPECT_EQ(body->count(), 4u) << "the adopted window is counted once, by the wrapper";

    char c;
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { body->readStrict(&c, 1); });
}

/// The hint is a text match on this repository's Poco. These pins fail the build's own tests the day
/// `SocketImpl::error` changes a word, which is the only way a text match stays honest.
TEST(CASRequestsConnectHint, PocoTextsArePinned)
{
    const auto text_of = [](int err)
    {
        try
        {
            Poco::Net::SocketImpl::error(err);
        }
        catch (const Poco::Exception & e)
        {
            return e.displayText();
        }
        return std::string("did not throw");
    };
    EXPECT_THAT(text_of(EADDRNOTAVAIL), testing::HasSubstr("Cannot assign requested address"));
    EXPECT_THAT(text_of(ECONNREFUSED), testing::HasSubstr("Connection refused"));
    EXPECT_THAT(text_of(EHOSTUNREACH), testing::HasSubstr("No route to host"));
    EXPECT_THAT(text_of(ENETUNREACH), testing::HasSubstr("Network is unreachable"));
    /// The fifth text is the connect poll's own: `SocketImpl::connect` throws
    /// `Poco::TimeoutException("connect timed out", ...)` (SocketImpl.cpp ~138).
    EXPECT_THAT(Poco::TimeoutException("connect timed out", "10.255.255.1:9").displayText(),
                testing::HasSubstr("connect timed out"));
}

#if USE_AWS_S3
TEST(CASRequestsConnectHint, ClassifierGuards)
{
    using Aws::S3::S3Errors;
    for (const char * text : {"Cannot assign requested address", "Connection refused", "No route to host",
                              "Network is unreachable", "connect timed out"})
    {
        const DB::S3Exception hinted(fmt::format("Poco::Exception. Code: 1000, e.code() = 99, {}: 10.0.0.1:9000", text),
                                     S3Errors::NETWORK_CONNECTION);
        EXPECT_TRUE(isConnectFailureHint(hinted)) << text;
        /// The same text under another S3 error is not a transport verdict.
        const DB::S3Exception other(String(text), S3Errors::INTERNAL_FAILURE);
        EXPECT_FALSE(isConnectFailureHint(other)) << text;
    }
    EXPECT_FALSE(isConnectFailureHint(DB::S3Exception("Timeout", S3Errors::NETWORK_CONNECTION)));
    EXPECT_FALSE(isConnectFailureHint(DB::S3Exception("Connection reset by peer", S3Errors::NETWORK_CONNECTION)));
    EXPECT_FALSE(isConnectFailureHint(Poco::TimeoutException("connect timed out")));
    EXPECT_FALSE(isConnectFailureHint(std::runtime_error("Connection refused")));
}

namespace
{

DB::S3::PocoHTTPClientConfiguration networkFailureClientConfiguration()
{
    DB::RemoteHostFilter remote_host_filter;
    return DB::S3::ClientFactory::instance().createClientConfiguration(
        "some-region",
        remote_host_filter,
        /* s3_max_redirects = */ 100,
        DB::S3::PocoHTTPClientConfiguration::RetryStrategy{.max_retries = 0},
        /* s3_slow_all_threads_after_network_error = */ true,
        /* s3_slow_all_threads_after_retryable_error = */ true,
        /* enable_s3_requests_logging = */ false,
        /* for_disk_s3 = */ false,
        /* opt_disk_name = */ {},
        /* request_throttler = */ {});
}

/// A client whose `PutObject` always fails with a `NETWORK_CONNECTION` `AWSError` carrying `text`
/// verbatim -- shaped exactly as `PocoHTTPClient` shapes a real connection failure (empty exception
/// name, the Poco text as the message) -- so a test built on it proves `WriteBufferFromS3`'s rethrow,
/// not a hand-built exception, is what `isConnectFailureHint` above actually has to classify.
struct NetworkFailurePutClient : DB::S3::Client
{
    explicit NetworkFailurePutClient(std::string text_)
        : DB::S3::Client(
            /*max_retries=*/100,
            DB::S3::ServerSideEncryptionKMSConfig(),
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("", ""),
            networkFailureClientConfiguration(),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            DB::S3::ClientSettings{
                .use_virtual_addressing = true,
                .disable_checksum = false,
                .gcs_issue_compose_request = false,
                .is_s3express_bucket = false,
            })
        , text(std::move(text_))
    {
    }

    Aws::S3::Model::PutObjectOutcome PutObject(const Aws::S3::Model::PutObjectRequest &) const override
    {
        return Aws::Client::AWSError<Aws::Client::CoreErrors>(Aws::Client::CoreErrors::NETWORK_CONNECTION, "", text, /*retryable=*/false);
    }

    std::string text;
};

}

/// The classifier above reads the Poco text a connection failure carries off an `S3Exception`; this
/// pins that the REAL `WriteBufferFromS3` rethrow every CAS conditional write goes through -- not a
/// hand-built exception -- hands the caller that text unchanged, under `NETWORK_CONNECTION`.
TEST(CASRequestsConnectHint, WriteBufferFromS3SurfacesTheConnectFailureTextUnchanged)
{
    for (const char * text : {"Cannot assign requested address", "Connection refused", "No route to host",
                              "Network is unreachable", "connect timed out"})
    {
        auto client = std::make_shared<NetworkFailurePutClient>(text);
        DB::WriteSettings write_settings;
        write_settings.object_storage_retry_profile = DB::ObjectStorageRetryProfile::SingleAttempt;
        DB::S3::S3RequestSettings request_settings;
        DB::WriteBufferFromS3 buffer(
            client, "bucket", "network_text", DB::DBMS_DEFAULT_BUFFER_SIZE, request_settings,
            /*blob_log_=*/nullptr, /*object_metadata_=*/std::nullopt, /*schedule_=*/{}, write_settings);
        buffer.write('A');
        try
        {
            buffer.finalize();
            FAIL() << "the injected failure must surface";
        }
        catch (const DB::S3Exception & e)
        {
            EXPECT_EQ(e.getS3ErrorCode(), Aws::S3::S3Errors::NETWORK_CONNECTION) << text;
            EXPECT_THAT(e.message(), testing::HasSubstr(text));
        }
    }
}

namespace
{
std::exception_ptr connectHint()
{
    return std::make_exception_ptr(DB::S3Exception(
        "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
        Aws::S3::S3Errors::NETWORK_CONNECTION));
}
}

TEST(CASRequestsConnectHint, HintedFailuresReissueWithoutARead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", connectHint());
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto hints_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load();
    const auto reissues_before = ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(backend->writeTotal(), 3u);
    EXPECT_EQ(backend->getTotal(), 0u);                 /// no settle read before the commit
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_EQ(clock.sleeps[0], 50u);                    /// the flat pause, twice
    EXPECT_EQ(clock.sleeps[1], 50u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load() - hints_before, 2u);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestReissue].load() - reissues_before, 2u);
}

TEST(CASRequestsConnectHint, ReissueMeetsPreconditionAndAdoptsOwnBytes)
{
    /// The hint was false: the write landed, its response was lost as a connect-failure text. The
    /// reissue meets 412 (the store now holds our OWN new incarnation, minted by the attempt whose
    /// response never arrived), one read follows and proves the bytes are ours.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->resetCounts();
        bool thrown = false;
        /// Runs after the write lands and before the caller ever sees a value, with no lock held --
        /// `InMemoryBackend::applyWrite` (CasInMemoryBackend.cpp) calls it right there.
        backend->onWriteCommitted("k", [&]
        {
            if (!thrown)
            {
                thrown = true;
                throw DB::S3Exception(
                    "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                    Aws::S3::S3Errors::NETWORK_CONNECTION);
            }
        });
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_TRUE(committed->resolved_by_read);
        EXPECT_EQ(committed->attempts_sent, 2u);
        EXPECT_EQ(backend->writeTotal(), 2u);
        EXPECT_EQ(backend->getTotal(), 1u);
        ASSERT_EQ(clock.sleeps.size(), 1u);
        EXPECT_EQ(clock.sleeps[0], 50u);
    }
    /// Different ETag, other bytes: a conflict, as today.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->failNextWriteWith("k", connectHint());
        /// A competitor lands during the flat pause: the engine's own sleep is the seam.
        bool competitor_landed = false;
        requests.setSleepFnForTest([&](uint64_t ms)
        {
            clock.sleepFn()(ms);
            if (!competitor_landed)
            {
                competitor_landed = true;
                auto other = requests.admit();
                orThrow(other.replace("k", "theirs", seen, Retry::standard()), "competitor");
            }
        });
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        EXPECT_TRUE(std::holds_alternative<Conflict>(result));
        EXPECT_EQ(backend->getTotal(), 1u);
    }
    /// The ORIGINAL ETag is still current after the hinted attempt: the reissue meets a CLEAN 412 (the
    /// store untouched, unlike sub-block 1's landed write), the settle read observes the original bytes
    /// still there under the original ETag, and a further reissue is what actually commits.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        const Etag seen = *orThrow(op.create("k", "v1", Retry::standard()), "create");
        backend->resetCounts();
        backend->failNextWriteWith("k", connectHint());   /// attempt 1: hint, flat pause, no read
        backend->refuseNextWrite("k");                    /// attempt 2: clean 412, store unchanged
        WriteResult result = op.replace("k", "v2", seen, Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_FALSE(committed->resolved_by_read);
        EXPECT_EQ(committed->attempts_sent, 3u);
        EXPECT_EQ(backend->writeTotal(), 3u);
        /// The read after attempt 2's 412 saw the precondition still satisfiable (the original ETag,
        /// untouched), so the loop reissued instead of adopting -- exactly one read, not zero.
        EXPECT_EQ(backend->getTotal(), 1u);
        ASSERT_EQ(clock.sleeps.size(), 2u);
        EXPECT_EQ(clock.sleeps[0], 50u);       /// the flat pause after attempt 1's hint
        EXPECT_LE(clock.sleeps[1], 200u);      /// the backoff after attempt 2's settle read
    }
}

TEST(CASRequestsConnectHint, OnceKeepsOneWriteAndOneRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto hints_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load();
    WriteResult result = op.create("k", "v", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
    /// The counter is "hint seen", recorded at classification: `Retry::once` never acts on it, but the
    /// attempt's transport error still named a failed connection.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load() - hints_before, 1u);
}

TEST(CASRequestsConnectHint, EarlierAmbiguityStillSettlesByRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousWrite("k");           /// attempt 1: ordinary ambiguity -> read, backoff
    /// Attempt 2's hint has to come from the hook, not a second `failNextWriteWith`: the armed-failure
    /// queue is checked BEFORE the ambiguous-key injection on every call, so a queued failure would win
    /// attempt 1 regardless of install order. `writeTotal()` ticks before the request is served, so it
    /// reads 2 while attempt 2 is in flight.
    bool hint_fired_on_second_attempt = false;
    backend->onBeforeWrite("k", [&]
    {
        if (backend->writeTotal() == 2)
        {
            /// The ordering claim in full: attempt 1's ambiguity must already have been settled by its
            /// read before attempt 2 -- the one place `sleeps[1] == 50u` alone could be fooled by a
            /// same-range jittered draw (`backoff(1)` is `uniform(0, 200)`, so a reversed order would
            /// false-green about once in 200 runs).
            EXPECT_EQ(backend->getTotal(), 1u) << "attempt 1's ambiguity read must already have run";
            hint_fired_on_second_attempt = true;
            throw DB::S3Exception(
                "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                Aws::S3::S3Errors::NETWORK_CONNECTION);
        }
    });
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    EXPECT_EQ(backend->getTotal(), 1u);
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_LE(clock.sleeps[0], 200u);     /// the backoff after attempt 1's ambiguity read
    EXPECT_EQ(clock.sleeps[1], 50u);      /// the flat pause after attempt 2's hint
    EXPECT_TRUE(hint_fired_on_second_attempt);
}

/// A single exception can be BOTH refusal-class (`isDefinitelyRefusedWrite` matches on the exception
/// NAME, independent of the S3 error code) and hint-text (`isConnectFailureHint` matches on the code
/// and the message): the classifier order, not the exception's shape, must decide which wins. An
/// earlier ambiguity of this inner write keeps the refusal from ending the call, but that must never
/// let the hint skip the read the earlier attempt still needs.
TEST(CASRequestsConnectHint, RefusalAfterAnEarlierAmbiguitySettlesByRead)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousWrite("k");           /// attempt 1: ordinary ambiguity -> read, backoff
    /// Attempt 2's refusal-and-hint exception has to come from the hook, not a second
    /// `failNextWriteWith`: the armed-failure queue is checked BEFORE the ambiguous-key injection on
    /// every call, so a queued failure would win attempt 1 regardless of install order (see the sibling
    /// `EarlierAmbiguityStillSettlesByRead` above). `writeTotal()` ticks before the request is served,
    /// so it reads 2 while attempt 2 is in flight.
    bool refusal_fired_on_second_attempt = false;
    backend->onBeforeWrite("k", [&]
    {
        if (backend->writeTotal() == 2)
        {
            EXPECT_EQ(backend->getTotal(), 1u) << "attempt 1's ambiguity read must already have run";
            refusal_fired_on_second_attempt = true;
            throw DB::S3Exception(
                "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                Aws::S3::S3Errors::NETWORK_CONNECTION, "MalformedXML");   /// refusal AND hint text
        }
    });
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto hints_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load();
    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    /// Two reads, one per settled attempt: a hint reissue for attempt 2 would have skipped its own
    /// read and left this at 1.
    EXPECT_EQ(backend->getTotal(), 2u);
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_LE(clock.sleeps[0], 200u);      /// backoff(1) after attempt 1's read
    EXPECT_LE(clock.sleeps[1], 400u);      /// backoff(2) after attempt 2's read -- today's verdict,
                                           /// never the flat 50 ms hint pause
    EXPECT_TRUE(refusal_fired_on_second_attempt);
    /// The refusal classification wins outright: a definite refusal is never a hint, so the counter
    /// must not move even though the exception's code and text also match `isConnectFailureHint`.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load() - hints_before, 0u);
}

/// A single exception can ALSO be both refreshable-credential-class (`isRefreshableCredentialError`
/// matches on the exception NAME, independent of the S3 error code) and hint-text
/// (`isConnectFailureHint` matches on the code and the message). The credential refresh drives the
/// reissue here, not the hint, so the hint counter must stay put.
TEST(CASRequestsConnectHint, RefreshedCredentialTextDoesNotDoubleCountTheHint)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    backend->failNextWriteWith("k", std::make_exception_ptr(DB::S3Exception(
        "Poco::Exception. Code: 1000, e.code() = 99, Connection refused: 10.0.0.1:9000",
        Aws::S3::S3Errors::NETWORK_CONNECTION, "ExpiredToken")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto hints_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 2u);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    /// The refresh -- not the hint's flat pause -- drove the reissue, so the hint counter must not move
    /// even though the exception's code and text also match `isConnectFailureHint`.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load() - hints_before, 0u);
}

/// The counter's ambiguity-precedence twin: the credential-owned reissue above requires
/// `!state.any_ambiguous`, so an earlier ambiguity of this inner write keeps it from applying even
/// though attempt 2's exception matches the refreshable-credential class. Attempt 2 is then reissued
/// by the ordinary hint mechanism instead -- flat-paused, and after the resolve read attempt 1 still
/// owes -- so the hint counter must count it.
TEST(CASRequestsConnectHint, CredentialRefreshAfterAnEarlierAmbiguityStillCountsTheHint)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    backend->injectAmbiguousWrite("k");           /// attempt 1: ordinary ambiguity -> read, backoff
    bool hint_fired_on_second_attempt = false;
    backend->onBeforeWrite("k", [&]
    {
        if (backend->writeTotal() == 2)
        {
            EXPECT_EQ(backend->getTotal(), 1u) << "attempt 1's ambiguity read must already have run";
            hint_fired_on_second_attempt = true;
            throw DB::S3Exception(
                "Poco::Exception. Code: 1000, e.code() = 99, Cannot assign requested address: 10.0.0.1:9000",
                Aws::S3::S3Errors::NETWORK_CONNECTION, "ExpiredToken");   /// hint AND credential-refreshable
        }
    });
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto hints_before = ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 3u);
    /// One read (attempt 1's) settles the earlier ambiguity; attempt 2's hint reissue skips its own
    /// read, exactly as `EarlierAmbiguityStillSettlesByRead` pins for a non-credential hint.
    EXPECT_EQ(backend->getTotal(), 1u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    ASSERT_EQ(clock.sleeps.size(), 2u);
    EXPECT_LE(clock.sleeps[0], 200u);     /// the backoff after attempt 1's ambiguity read
    EXPECT_EQ(clock.sleeps[1], 50u);      /// the flat pause after attempt 2's hint, not a credential backoff
    EXPECT_TRUE(hint_fired_on_second_attempt);
    /// The hint mechanism, not a credential-owned reissue, actually resent this attempt, so the counter
    /// counts it even though the exception's name also matches the refreshable-credential class.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestConnectFailureHint].load() - hints_before, 1u);
}

TEST(CASRequestsConnectHint, GatesRefuseTheReissue)
{
    /// Deadline: hints until the window closes.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        for (int i = 0; i < 100; ++i)
            backend->failNextWriteWith("k", connectHint());
        auto requests = makeRequests(backend, clock);
        requests.setAttemptReservationForTest(1'000);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::within(3'000));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_TRUE(gave_up->sent_any);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
    /// Fence: the fence trips during the pause.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", connectHint());
        bool lost = false;
        Fence fence{
            [] { return uint64_t{1}; },
            [&](uint64_t, uint64_t) { return lost ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
            [](uint64_t) {}};
        auto requests = makeRequests(backend, clock, fence);
        requests.setSleepFnForTest([&](uint64_t ms) { clock.sleepFn()(ms); lost = true; });
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    }
    /// Two envelopes exactly for the attempt; today's ambiguous path would still have its read
    /// envelope (2000 >= 1000), the hint path gives up instead -- the documented deadline-edge
    /// difference.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", connectHint());
        auto requests = makeRequests(backend, clock);
        requests.setAttemptReservationForTest(1'000);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::within(2'000));
        const auto * gave_up = std::get_if<GaveUp>(&result);
        ASSERT_NE(gave_up, nullptr);
        EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
        EXPECT_TRUE(gave_up->sent_any);
        EXPECT_EQ(backend->getTotal(), 0u);
    }
}

TEST(CASRequestsConnectHint, AmbiguityAfterHintsStartsAtFirstBackoff)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", connectHint());
    backend->failNextWriteWith("k", std::make_exception_ptr(Poco::TimeoutException("the write timed out")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
    ASSERT_EQ(clock.sleeps.size(), 3u);
    EXPECT_EQ(clock.sleeps[0], 50u);
    EXPECT_EQ(clock.sleeps[1], 50u);
    /// `backoff(1)` is full jitter over [0, 200] ms (`CasRetry.h`): the hints did not advance the index.
    EXPECT_LE(clock.sleeps[2], 200u);
}

TEST(CASRequestsFuse, MatcherPrecedence)
{
    using Aws::S3::S3Errors;
    /// The generic transport-timeout text is Poco's exception name, pinned here.
    EXPECT_THAT(Poco::TimeoutException("the socket").displayText(), testing::StartsWith("Timeout"));
    const DB::S3Exception fuse("Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket", S3Errors::NETWORK_CONNECTION);
    EXPECT_TRUE(isFirstAttemptFuseTimeout(fuse, 1));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(fuse, 2));
    const DB::S3Exception hint("Poco::Exception. Code: 1000, e.code() = 0, Timeout: connect timed out: 10.0.0.1:9", S3Errors::NETWORK_CONNECTION);
    EXPECT_FALSE(isFirstAttemptFuseTimeout(hint, 1));     /// the connect-failure hint owns it
    EXPECT_TRUE(isConnectFailureHint(hint));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(DB::S3Exception("Connection reset by peer", S3Errors::NETWORK_CONNECTION), 1));
    EXPECT_FALSE(isFirstAttemptFuseTimeout(DB::S3Exception("Timeout", S3Errors::INTERNAL_FAILURE), 1));
}

namespace
{
std::exception_ptr fuseTimeout()
{
    return std::make_exception_ptr(DB::S3Exception("Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket",
                                                   Aws::S3::S3Errors::NETWORK_CONNECTION));
}
}

TEST(CASRequestsFuse, FirstAttemptTimeoutReissuesWithoutSleep)
{
    /// Write: the settle read still runs (the request may have been sent), then a no-sleep reissue.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", fuseTimeout());
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        const auto * committed = std::get_if<Committed>(&result);
        ASSERT_NE(committed, nullptr);
        EXPECT_EQ(committed->attempts_sent, 2u);
        EXPECT_EQ(backend->getTotal(), 1u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
    /// Read: no settle read, no sleep.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        orThrow(op.create("k", "v", Retry::standard()), "seed");
        backend->resetCounts();
        backend->failNextReadWith("k", fuseTimeout());
        EXPECT_TRUE(op.read("k", Retry::standard()).has_value());
        EXPECT_EQ(backend->getTotal(), 2u);
        EXPECT_TRUE(clock.sleeps.empty());
    }
    /// LIST: no sleep either. LIST has no `failNextWith`-style armed queue (only write/read/head do),
    /// so a small backend that throws the fuse on its first LIST and records the physical attempt
    /// number stands in.
    {
        struct ListFuseOnceBackend : CountingBackend
        {
            bool armed = true;
            std::vector<size_t> list_attempts;
            RawListPage list(const String & prefix, const String & cursor, size_t limit, TransportAccess & access) override
            {
                list_attempts.push_back(access.attemptNo());
                if (armed)
                {
                    armed = false;
                    std::rethrow_exception(fuseTimeout());
                }
                return CountingBackend::list(prefix, cursor, limit, access);
            }
        };
        FakeClock clock;
        auto backend = std::make_shared<ListFuseOnceBackend>();
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        (void)op.list("p/", "", 10, Retry::standard());
        EXPECT_EQ(backend->list_attempts, (std::vector<size_t>{1, 2}));
        EXPECT_TRUE(clock.sleeps.empty());
    }
    /// Attempts 1 and 2 failing: attempt 2 is not a first attempt, so exactly one sleep, after it.
    {
        FakeClock clock;
        auto backend = std::make_shared<CountingBackend>();
        backend->failNextWriteWith("k", fuseTimeout());
        backend->failNextWriteWith("k", fuseTimeout());
        auto requests = makeRequests(backend, clock);
        auto op = requests.admit();
        WriteResult result = op.create("k", "v", Retry::standard());
        ASSERT_TRUE(std::holds_alternative<Committed>(result));
        EXPECT_EQ(std::get<Committed>(result).attempts_sent, 3u);
        EXPECT_EQ(clock.sleeps.size(), 1u);
    }
}

TEST(CASRequestsFuse, GatesRefuseTheZeroPauseReissue)
{
    /// `setAttemptReservationForTest(1'000)`: the write's own admission reserves two envelopes
    /// (`reservedFor(0, 2) == 2000`), which matches a 2000 ms window exactly -- `fits` is `needed <=
    /// remaining`, so the boundary admits. The settle read that follows the fuse reserves only one
    /// envelope (`reservedFor(0, 1) == 1000`), which still fits even after the clock below has moved.
    /// What must NOT fit is the zero-pause reissue's own `reservedFor(0, 2) == 2000`. `FakeClock` never
    /// moves on its own -- only a sleep advances it, and this path sleeps none -- so a naive `now()`
    /// would see the SAME instant at every one of the four calls this operation makes (the initial
    /// `bind`, the write's own admission, the settle read's admission, the reissue's admission) and
    /// wrongly admit the reissue too. A real failing attempt spends wall time even though it never
    /// lands, so this fixture's clock counts its own calls and adds 1 ms starting from the THIRD one
    /// (the settle read's admission) onward: late enough that the write's own admission still sees the
    /// pristine window, early enough that the reissue's admission sees one fewer millisecond than it
    /// needs.
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", fuseTimeout());
    int now_calls = 0;
    auto requests = makeRequests(backend, clock);
    requests.setAttemptReservationForTest(1'000);
    requests.setNowFnForTest([&clock, &now_calls]() -> uint64_t
    {
        ++now_calls;
        return clock.now + (now_calls <= 2 ? 0 : 1);
    });
    auto op = requests.admit();
    WriteResult result = op.create("k", "v", Retry::within(2'000));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_TRUE(clock.sleeps.empty()) << "the zero-pause reissue never sleeps, even when refused";
    /// The fence, not the deadline, refuses the zero-pause reissue: three `Fence::admit` calls happen
    /// in this scenario -- the write's own admission, the settle read's admission, and the reissue's
    /// admission -- in that order, so tripping the fence on the THIRD call refuses only the reissue,
    /// after the write attempt and its settle read both already went through.
    {
        FakeClock fence_clock;
        auto fence_backend = std::make_shared<CountingBackend>();
        fence_backend->failNextWriteWith("k", fuseTimeout());
        int admit_calls = 0;
        Fence fence{
            [] { return uint64_t{1}; },
            [&](uint64_t, uint64_t) { return ++admit_calls >= 3 ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
            [](uint64_t) {}};
        auto fence_requests = makeRequests(fence_backend, fence_clock, fence);
        auto fence_op = fence_requests.admit();
        WriteResult fence_result = fence_op.create("k", "v", Retry::standard());
        const auto * fence_gave_up = std::get_if<GaveUp>(&fence_result);
        ASSERT_NE(fence_gave_up, nullptr);
        EXPECT_EQ(fence_gave_up->why, GaveUp::Why::FenceLost);
        EXPECT_TRUE(fence_gave_up->sent_any);
        EXPECT_EQ(fence_backend->writeTotal(), 1u) << "the fence refuses before a second write is ever sent";
    }
    /// `Retry::once()` never performs a second attempt.
    auto once_backend = std::make_shared<CountingBackend>();
    once_backend->failNextWriteWith("k", fuseTimeout());
    auto once_requests = makeRequests(once_backend, clock);
    auto once_op = once_requests.admit();
    (void)once_op.create("k", "v", Retry::once());
    EXPECT_EQ(once_backend->writeTotal(), 1u);
}

TEST(CASRequestsFuse, ReadLoopZeroPauseKeepsTheBackoffIndex)
{
    struct ReadAttemptRecordingBackend : CountingBackend
    {
        std::vector<size_t> read_attempts;
        std::optional<Raw> read(const String & key, TransportAccess & access) override
        {
            read_attempts.push_back(access.attemptNo());
            return CountingBackend::read(key, access);
        }
    };
    FakeClock clock;
    auto backend = std::make_shared<ReadAttemptRecordingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "v", Retry::standard()), "seed");
    backend->read_attempts.clear();
    backend->failNextReadWith("k", fuseTimeout());
    backend->failNextReadWith("k", std::make_exception_ptr(Poco::TimeoutException("attempt 2: an ordinary fault")));
    EXPECT_TRUE(op.read("k", Retry::standard()).has_value());
    ASSERT_EQ(clock.sleeps.size(), 1u);
    /// The one sleep is `backoff(1)`: the zero-pause reissue did not advance the index.
    EXPECT_LE(clock.sleeps[0], 200u);   /// `backoff(1)` is full jitter over [0, 200] ms
    /// The transport still sees every physical attempt: the zero-pause reissue (attempt 2) advances
    /// `attempt_no` alone, so attempt 3 -- reached only after the one ordinary backoff -- follows it,
    /// not a second attempt 1.
    EXPECT_EQ(backend->read_attempts, (std::vector<size_t>{1, 2, 3}));
}

/// `Retry::once` forbids the REISSUE, not the observation: a fuse a single-attempt read hits still
/// counts (the write path already counts at classification, before its own single-attempt check), it
/// just throws unchanged instead of re-sending.
TEST(CASRequestsFuse, ReadUnderOnceCountsTheFuseWithoutReissuing)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextReadWith("k", fuseTimeout());
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto fuses_before = ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load();
    expectThrowsCode(DB::ErrorCodes::S3_ERROR, [&] { (void)op.read("k", Retry::once()); });
    EXPECT_EQ(backend->getTotal(), 1u) << "Retry::once performs no second attempt";
    EXPECT_TRUE(clock.sleeps.empty());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load() - fuses_before, 1u);
}

/// The fuse counter's credential-refresh twin of `CASRequestsConnectHint.RefreshedCredentialTextDoesNotDoubleCountTheHint`:
/// a first attempt whose exception is both fuse-text and refreshable-credential-name must be counted
/// as the credential reissue it actually is, not also as a fuse.
TEST(CASRequestsFuse, RefreshedCredentialTextDoesNotDoubleCountTheFuse)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    backend->setRefreshCredentialsResult(true);
    backend->failNextWriteWith("k", std::make_exception_ptr(DB::S3Exception(
        "Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket",
        Aws::S3::S3Errors::NETWORK_CONNECTION, "ExpiredToken")));
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    const auto fuses_before = ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load();

    WriteResult result = op.create("k", "v", Retry::standard());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_NE(committed, nullptr);
    EXPECT_EQ(committed->attempts_sent, 2u);
    EXPECT_FALSE(committed->resolved_by_read);
    EXPECT_EQ(backend->getTotal(), 0u);
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    /// The refresh -- not the fuse's immediate reissue -- drove the resend, so the fuse counter must not
    /// move even though the exception's code and text also match `isFirstAttemptFuseTimeout`.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load() - fuses_before, 0u);
}

/// The read loop's own twin of `RefreshedCredentialTextDoesNotDoubleCountTheFuse`: a first read attempt
/// whose exception is both fuse-text and refreshable-credential-name is a credential reissue, not a
/// fuse, so the counter must not move even though the reissue itself is immediate, exactly like a fuse.
TEST(CASRequestsFuse, ReadRefreshedCredentialTextDoesNotDoubleCountTheFuse)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    orThrow(op.create("k", "v", Retry::standard()), "seed");
    backend->resetCounts();
    backend->setRefreshCredentialsResult(true);
    backend->failNextReadWith("k", std::make_exception_ptr(DB::S3Exception(
        "Poco::Exception. Code: 1000, e.code() = 0, Timeout: the socket",
        Aws::S3::S3Errors::NETWORK_CONNECTION, "ExpiredToken")));
    const auto fuses_before = ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load();

    const auto seen = op.read("k", Retry::standard());
    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(seen->bytes, "v");
    EXPECT_EQ(backend->getTotal(), 2u) << "the failed attempt and its immediate reissue both reached the store";
    EXPECT_EQ(backend->refreshCredentialsCalls(), 1u);
    EXPECT_TRUE(clock.sleeps.empty());
    /// The refresh -- not the fuse's immediate reissue -- drove the resend, so the fuse counter must not
    /// move even though the exception's code and text also match `isFirstAttemptFuseTimeout`.
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASRequestFirstAttemptFuse].load() - fuses_before, 0u);
}

#endif

TEST(CASRequestBudget, EnvelopeIsValidatedNotTheBareAttempt)
{
    CasRequestBudget budget{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = 1000};
    EXPECT_EQ(budget.attemptEnvelopeMs(), 7000u);
    EXPECT_EQ((CasRequestBudget{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = std::nullopt}.attemptEnvelopeMs()), 5000u);
    /// Defaults with the default TTL / period are accepted.
    EXPECT_NO_THROW(validateCasRequestBudget(budget, 30000, 10000, /*background_renewal=*/true));
    /// A zero attempt timeout would reserve nothing while the request keeps the disk's own timeout.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(CasRequestBudget{.attempt_timeout_ms = 0, .lease_safety_margin_ms = 2000,
                                                   .connect_timeout_cap_ms = std::nullopt}, 30000, 10000, true);
    });
    /// The old inequality (attempt <= TTL - margin - period: 5000 <= 13000) accepted this; two envelopes
    /// of 15 s do not fit a 25 s lease behind a 10 s period and a 2 s margin.
    const CasRequestBudget wide{.attempt_timeout_ms = 5000, .lease_safety_margin_ms = 2000, .connect_timeout_cap_ms = 5000};
    try
    {
        validateCasRequestBudget(wide, 25000, 10000, true);
        FAIL() << "must refuse";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_THAT(e.message(), testing::HasSubstr("envelope"));
        EXPECT_THAT(e.message(), testing::HasSubstr("15000"));
    }
    /// Without background renewal only `envelope + margin < TTL` applies (15000 + 2000 < 25000).
    EXPECT_NO_THROW(validateCasRequestBudget(wide, 25000, 10000, /*background_renewal=*/false));
    /// Saturation: absurd values fail closed rather than wrap.
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    {
        validateCasRequestBudget(CasRequestBudget{.attempt_timeout_ms = std::numeric_limits<uint64_t>::max(),
                                                   .lease_safety_margin_ms = 1, .connect_timeout_cap_ms = 1},
                                  30000, 10000, true);
    });
}

TEST(CASRequests, ReservationIsTheEnvelope)
{
    struct EnvelopeBackend : InMemoryBackend
    {
        uint64_t attemptTimeoutMs() const override { return 5000; }
        uint64_t attemptEnvelopeMs() const override { return 7000; }
    };
    FakeClock clock;
    auto backend = std::make_shared<EnvelopeBackend>();
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();
    /// A write reserves two envelopes: 14 s fits a 14 s window, 13.999 s does not.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::within(14'000))));
    const WriteResult refused = op.create("k2", "v", Retry::within(13'999));
    const auto * gave_up = std::get_if<GaveUp>(&refused);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_FALSE(gave_up->sent_any);
}

/// Every `Backend` decorator that forwards `attemptTimeoutMs` to an inner backend must forward
/// `attemptEnvelopeMs` too, or the default (`attemptEnvelopeMs() { return attemptTimeoutMs(); }`)
/// silently drops the inner backend's connect contribution -- exactly the gap `Pool::open`'s
/// `InstrumentedBackend` wrapper had. Pin the forwarding through the same engine construction
/// production uses.
TEST(CASRequests, ReservationIsTheEnvelopeThroughInstrumentedBackend)
{
    struct EnvelopeBackend : InMemoryBackend
    {
        uint64_t attemptTimeoutMs() const override { return 5000; }
        uint64_t attemptEnvelopeMs() const override { return 7000; }
    };
    FakeClock clock;
    auto inner = std::make_shared<EnvelopeBackend>();
    auto wrapped = std::make_shared<InstrumentedBackend>(inner);
    ASSERT_EQ(wrapped->attemptTimeoutMs(), 5000u);
    ASSERT_EQ(wrapped->attemptEnvelopeMs(), 7000u) << "InstrumentedBackend must forward the envelope, not fall back to the bare attempt timeout";
    auto requests = makeRequests(wrapped, clock);
    auto op = requests.admit();
    /// Same boundary as ReservationIsTheEnvelope, now through the wrapper `Pool::open` actually uses.
    EXPECT_TRUE(std::holds_alternative<Committed>(op.create("k", "v", Retry::within(14'000))));
    const WriteResult refused = op.create("k2", "v", Retry::within(13'999));
    const auto * gave_up = std::get_if<GaveUp>(&refused);
    ASSERT_NE(gave_up, nullptr);
    EXPECT_FALSE(gave_up->sent_any);
}
