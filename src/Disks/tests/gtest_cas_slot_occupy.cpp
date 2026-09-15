#include <gtest/gtest.h>

#include "config.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Poco/Exception.h>

#include <variant>

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// ================================================================================================
/// The ref lane's slot occupy: ONE conditional create of a write-once ref-log key, `Retry::once()`,
/// on an operation the caller resumed under the generation its transaction was admitted at. It is
/// what every epoch-seal writer and every wedge retry issues, so these tests pin the shape those two
/// callers depend on -- the four alternatives and the request count behind each -- rather than the
/// engine's general write contract, which `gtest_cas_requests.cpp` owns.
///
/// Adjudicating whether a conflicting occupant is "mine" is entirely the CALLER's job (the
/// `CaCasMountCore` `mine` contract: byte equality, never a shape or generation match); nothing here
/// compares bytes for meaning.
/// ================================================================================================

namespace
{

/// Deletes the key the INSTANT its own conditional create conflicts, modelling "the occupant that
/// caused the conflict vanished before the settling read" -- a race a real backend can produce (e.g.
/// GC reclaiming an already-condemned object) that the call must survive by reporting what it saw,
/// never a fabricated commit.
class VanishOnConflictBackend : public CountingBackend
{
public:
    std::expected<String, DB::Cas::Backend::RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        DB::Cas::TransportAccess & access) override
    {
        auto result = CountingBackend::write(key, bytes, expected_value, access);
        if (!result.has_value())
        {
            if (const auto meta = CountingBackend::head(key, access))
                CountingBackend::remove(key, meta->value, access);
        }
        return result;
    }
};

/// Throws a deterministic LOCAL failure (`BAD_ARGUMENTS`, in `isDeterministicLocalFailure`'s set) on
/// the first write -- a backend-level programming bug, distinct from a whitelisted synchronous
/// rejection, which the store gives as an answer and the engine reports as `Refused`.
class LocalFailureOnceBackend : public CountingBackend
{
public:
    bool fail_once = true;

    std::expected<String, DB::Cas::Backend::RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        DB::Cas::TransportAccess & access) override
    {
        if (fail_once)
        {
            fail_once = false;
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "scripted deterministic local failure");
        }
        return CountingBackend::write(key, bytes, expected_value, access);
    }
};

/// Withdraws the caller's liveness the instant a conditional create conflicts, so the settling read
/// is the first request the operation is no longer admitted for.
class WithdrawAdmissionOnConflictBackend : public CountingBackend
{
public:
    bool live = true;

    std::expected<String, DB::Cas::Backend::RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        DB::Cas::TransportAccess & access) override
    {
        auto result = CountingBackend::write(key, bytes, expected_value, access);
        if (!result.has_value())
            live = false;
        return result;
    }
};

/// The occupant a conflict names, or null when the result is not a conflict that observed one.
const Object * conflictObject(const WriteResult & result)
{
    const auto * conflict = std::get_if<Conflict>(&result);
    return conflict ? std::get_if<Object>(&conflict->seen) : nullptr;
}

}

TEST(CASSlotOccupy, AbsentKeyCommitsWithOneRequest)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();

    const WriteResult result = op.create("k", "payload", Retry::once());
    const auto * committed = std::get_if<Committed>(&result);
    ASSERT_TRUE(committed != nullptr);
    EXPECT_EQ(committed->attempts_sent, 1u);
    EXPECT_FALSE(committed->resolved_by_read) << "an unambiguous create is proven by its own response";

    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 0u);
    EXPECT_EQ(backend->headCount("k"), 0u);

    CasOperation reader = requests.admit();
    const auto landed = reader.read("k", Retry::once());
    ASSERT_TRUE(landed.has_value());
    EXPECT_EQ(landed->bytes, "payload");
}

TEST(CASSlotOccupy, PreExistingKeyConflictsWithExactBytesAndIncarnationInTwoRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());

    CasOperation seeder = requests.admit();
    const WriteResult seeded = seeder.create("k", "occupant-bytes", Retry::once());
    const auto * seeded_committed = std::get_if<Committed>(&seeded);
    ASSERT_TRUE(seeded_committed != nullptr);
    const Etag seeded_incarnation = seeded_committed->etag;
    backend->resetCounts();

    CasOperation op = requests.admit();
    const WriteResult result = op.create("k", "my-attempt-bytes", Retry::once());
    const Object * occupant = conflictObject(result);
    ASSERT_TRUE(occupant != nullptr) << "the settling read must have named the occupant";
    EXPECT_EQ(occupant->bytes, "occupant-bytes");
    EXPECT_EQ(occupant->etag, seeded_incarnation);

    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
    EXPECT_EQ(backend->headCount("k"), 0u)
        << "exactly one write and one settling read -- a HEAD-then-read implementation must fail this";

    CasOperation reader = requests.admit();
    const auto current = reader.read("k", Retry::once());
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(current->bytes, "occupant-bytes") << "a conflict never overwrites or appends";
}

TEST(CASSlotOccupy, AmbiguousWriteThatLandedNothingGivesUpHavingSentOne)
{
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousWrite("k");
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();

    const WriteResult result = op.create("k", "payload", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_TRUE(gave_up != nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Unresolved);
    EXPECT_TRUE(gave_up->sent_any) << "the ambiguous attempt itself was sent -- this is never the pre-attempt case";
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(gave_up->last_seen));

    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
    CasOperation reader = requests.admit();
    EXPECT_FALSE(reader.head("k", Retry::once()).has_value()) << "the injected fault must not create anything";
}

TEST(CASSlotOccupy, ConflictThenVanishGivesUpRatherThanFabricatingACommit)
{
    auto backend = std::make_shared<VanishOnConflictBackend>();
    CasRequests requests(backend, Fence::open());

    CasOperation seeder = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(seeder.create("k", "occupant-bytes", Retry::once())));
    backend->resetCounts();

    CasOperation op = requests.admit();
    const WriteResult result = op.create("k", "my-attempt-bytes", Retry::once());
    /// Nothing of ours was ever ambiguous, so an absence settles the call as a conflict against an
    /// occupant that is no longer there -- never as a commit.
    const auto * conflict = std::get_if<Conflict>(&result);
    ASSERT_TRUE(conflict != nullptr);
    EXPECT_TRUE(std::holds_alternative<ProvenAbsent>(conflict->seen));

    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);
}

TEST(CASSlotOccupy, LivenessRefusalBeforeTheAttemptSendsNothing)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit([] { return false; });

    const WriteResult result = op.create("k", "payload", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_TRUE(gave_up != nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_FALSE(gave_up->sent_any) << "the whole point: the key is provably unwritten";

    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u);
    CasRequests open_requests(backend, Fence::open());
    CasOperation reader = open_requests.admit();
    EXPECT_FALSE(reader.head("k", Retry::once()).has_value()) << "never a lie of committed -- the key must be untouched";
}

/// The deadline is the OTHER pre-attempt refusal: a fake clock proves it fires from elapsed time
/// alone, under a fence that always says yes.
TEST(CASSlotOccupy, ExhaustedPolicyDeadlineRefusesBeforeTheAttempt)
{
    auto backend = std::make_shared<CountingBackend>();
    uint64_t clock = 0;
    CasRequests requests(backend, Fence::open(),
                         [&clock]() -> uint64_t { const uint64_t t = clock; clock += 1000; return t; });
    requests.setAttemptReservationForTest(50);
    CasOperation op = requests.admit();

    /// Entry `now_ms()` is 0, so the bound is 500; the loop's own `now_ms()` then reads 1000.
    const WriteResult result = op.create("k", "payload", Retry::within(500));
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_TRUE(gave_up != nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::Deadline);
    EXPECT_FALSE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 0u);
    EXPECT_EQ(backend->getTotal(), 0u)
        << "zero requests -- the deadline refuses before any I/O, exactly as the liveness gate does";
}

/// A whitelisted synchronous rejection PROVES the request was never applied, so the engine reports it
/// as a value and settles nothing by reading. Guarded to `USE_AWS_S3` builds ONLY: the classification
/// lives entirely inside `isDefinitelyRefusedWrite`'s own `#if USE_AWS_S3`, so without it this would
/// silently exercise the ambiguity path instead of the refusal it names.
#if USE_AWS_S3
TEST(CASSlotOccupy, DefiniteStoreRefusalIsAValueAndSettlesNothing)
{
    auto backend = std::make_shared<CountingBackend>();
    backend->failNextWriteWith("k", std::make_exception_ptr(
        DB::S3Exception("simulated malformed request", Aws::S3::S3Errors::UNKNOWN, "MalformedXML")));
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();

    const WriteResult result = op.create("k", "payload", Retry::once());
    EXPECT_TRUE(std::holds_alternative<Refused>(result));
    EXPECT_EQ(backend->getCount("k"), 0u) << "a proven refusal must never trigger a settling read";
}
#endif

/// A deterministic LOCAL failure is the one thing the write surface still reports by exception:
/// reissuing only replays it, and folding it into an outcome would bury the root cause.
TEST(CASSlotOccupy, DeterministicLocalFailurePropagatesWithoutSettling)
{
    auto backend = std::make_shared<LocalFailureOnceBackend>();
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit();

    bool threw = false;
    try
    {
        op.create("k", "payload", Retry::once());
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS) << "the ORIGINAL exception must propagate unchanged";
    }
    EXPECT_TRUE(threw) << "a deterministic local failure must propagate, never return an outcome";
    EXPECT_FALSE(backend->fail_once);
    EXPECT_EQ(backend->getCount("k"), 0u);
}

/// A commit whose admission was withdrawn while it was in flight is reported as unresolved, never as
/// committed: the object may well exist, and the caller has to resolve the key rather than act on a
/// claim made under an incarnation it no longer holds. This is the OPPOSITE of the retired
/// slot-occupy primitive's single-pre-attempt-check contract, and it is what lets the ref lane's
/// wedge stay wedged instead of installing against a fence it has already lost.
TEST(CASSlotOccupy, AdmissionLostAfterTheWriteIsNeverReportedCommitted)
{
    auto backend = std::make_shared<CountingBackend>();
    bool live = true;
    backend->onWriteCommitted("k", [&live] { live = false; });
    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit([&live] { return live; });

    const WriteResult result = op.create("k", "payload", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_TRUE(gave_up != nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_TRUE(gave_up->sent_any) << "the write was sent, and it landed -- the caller must resolve the key";

    /// The object IS durable; only the claim about it is refused.
    CasRequests open_requests(backend, Fence::open());
    CasOperation reader = open_requests.admit();
    const auto landed = reader.read("k", Retry::once());
    ASSERT_TRUE(landed.has_value());
    EXPECT_EQ(landed->bytes, "payload");
}

/// A conflict needs a second request to name its occupant. Admission may disappear while the
/// conditional create is in flight; the settling read must then not start at all.
TEST(CASSlotOccupy, AdmissionLostAfterAConflictPreventsTheSettlingRead)
{
    auto backend = std::make_shared<WithdrawAdmissionOnConflictBackend>();
    CasRequests seed_requests(backend, Fence::open());
    CasOperation seeder = seed_requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(seeder.create("k", "existing", Retry::once())));
    backend->resetCounts();

    CasRequests requests(backend, Fence::open());
    CasOperation op = requests.admit([&backend] { return backend->live; });
    const WriteResult result = op.create("k", "attempt", Retry::once());
    const auto * gave_up = std::get_if<GaveUp>(&result);
    ASSERT_TRUE(gave_up != nullptr);
    EXPECT_EQ(gave_up->why, GaveUp::Why::FenceLost);
    EXPECT_TRUE(gave_up->sent_any);
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 0u)
        << "the settling read started after admission was withdrawn";
}

/// The wedge-adoption input shape: an earlier ambiguous attempt of the SAME key and bytes landed, and
/// a later flush issues a fresh create for it. The first call settles it by reading its own bytes; the
/// second sees them as an ordinary occupant, which is what the lane's `mine` adjudication consumes.
TEST(CASSlotOccupy, OwnLandedAmbiguousWriteIsObservedOnTheNextAttempt)
{
    auto backend = std::make_shared<CountingBackend>();
    backend->injectAmbiguousLandedWrite("k");
    CasRequests requests(backend, Fence::open());

    CasOperation first_op = requests.admit();
    const WriteResult first = first_op.create("k", "my-bytes", Retry::once());
    const auto * committed = std::get_if<Committed>(&first);
    ASSERT_TRUE(committed != nullptr) << "the write landed; the settling read proves it";
    EXPECT_TRUE(committed->resolved_by_read);
    const Etag landed_incarnation = committed->etag;
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount("k"), 1u);

    CasOperation second_op = requests.admit();
    const WriteResult second = second_op.create("k", "my-bytes", Retry::once());
    const Object * occupant = conflictObject(second);
    ASSERT_TRUE(occupant != nullptr);
    EXPECT_EQ(occupant->bytes, "my-bytes");
    EXPECT_EQ(occupant->etag, landed_incarnation) << "both calls must observe the SAME landed incarnation";
    EXPECT_EQ(backend->writeTotal(), 2u);
    EXPECT_EQ(backend->getCount("k"), 2u);
}
