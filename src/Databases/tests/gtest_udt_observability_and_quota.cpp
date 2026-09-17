#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/DatabaseResourceQuota.h>

#include <DataTypes/UDT/ResourceLimits.h>

#include <base/scope_guard.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event UDTAuthorityRootPublications;
extern const Event UDTAuthorityPublishedDeterministicCatalogBytes;
extern const Event UDTManifestEntries;
extern const Event UDTManifestBytes;
extern const Event UDTManifestDispatchCopies;
extern const Event UDTManifestDispatchBytes;
extern const Event UDTManifestReceiverAdmissions;
}

namespace CurrentMetrics
{
extern const Metric UDTLiveCatalogAndCacheBytes;
}

namespace DB::FailPoints
{
extern const char udt_authority_prepared_publication_failure[];
}

namespace DB::UDT
{
namespace
{

UUID observabilityUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

AuthorityRoot::Ptr makeEmptyRoot(UUID database_uuid)
{
    const auto inventory = buildAuthorityInventorySummary({});
    auto graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    const auto state = makeAuthorityState(
        database_uuid, 1, definition_authority_capability_mask, inventory.leaf_count, inventory.merkle_radix_root, graph->computeRoot());
    const std::vector<Definition::Ptr> definitions;
    const std::vector<Record> records;
    const std::vector<SidecarExpectationRecord> expectations;
    return AuthorityRootBuilder::buildInitialAdmission(state, 1, definitions, records, expectations, std::move(graph));
}

ProfileEvents::Count eventValue(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event];
}

class ThrowingPublicationObserver final : public IAtomicAuthorityPublicationObserver
{
public:
    std::unique_ptr<PreparedTransition> prepareAuthorityPublication(const AuthorityRoot &, const AuthorityRoot &) override
    {
        ++preparations;
        throw std::runtime_error("observer preparation failed");
    }

    UInt64 preparations = 0;
};

TEST(UDTObservability, CountsOnlyInstalledRootsAndCanonicalDatabaseBytes)
{
    const UUID database_uuid = observabilityUUID(0x8100, 1);
    auto initial_root = makeEmptyRoot(database_uuid);
    const UInt64 canonical_bytes
        = initial_root->getDatabaseResourceQuota().getUsage().get(ResourceLimit::DeterministicCatalogBytesPerDatabase);
    ASSERT_GT(canonical_bytes, 0);

    const auto publications_before = eventValue(ProfileEvents::UDTAuthorityRootPublications);
    const auto bytes_before = eventValue(ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes);
    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(initial_root));

    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityRootPublications) - publications_before, 1);
    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes) - bytes_before, canonical_bytes);

    AuthorityRoot::Ptr replacement;
    {
        auto snapshot = authority.acquireCurrentRoot();
        replacement = snapshot->cloneForExactRepair();
    }
    auto prepared = authority.preparePublication(std::move(replacement));
    authority.publish(std::move(prepared));

    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityRootPublications) - publications_before, 2);
    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes) - bytes_before, 2 * canonical_bytes);
}

TEST(UDTObservability, FailedPreparationUnwindsServerChargeBeforeReturningAndPublishesNoMetric)
{
    const UUID database_uuid = observabilityUUID(0x8100, 2);
    auto initial_root = makeEmptyRoot(database_uuid);
    const UInt64 server_maximum
        = initial_root->getDatabaseResourceQuota().getLimits().get(ResourceLimit::LiveCatalogAndCacheBytesPerServer);
    auto tracker = ServerResourceQuotaTracker::acquireProcessTracker(server_maximum);
    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(initial_root));
    const auto initial_state = tracker->getState();
    ASSERT_GT(initial_state.charged_bytes, 0);

    const auto publications_before = eventValue(ProfileEvents::UDTAuthorityRootPublications);
    const auto bytes_before = eventValue(ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes);
    AuthorityRoot::Ptr replacement;
    {
        auto snapshot = authority.acquireCurrentRoot();
        replacement = snapshot->cloneForExactRepair();
    }

    FailPointInjection::enableFailPoint(FailPoints::udt_authority_prepared_publication_failure);
    SCOPE_EXIT({ FailPointInjection::disableFailPoint(FailPoints::udt_authority_prepared_publication_failure); });
    EXPECT_THROW(static_cast<void>(authority.preparePublication(std::move(replacement))), AtomicAuthorityError);

    EXPECT_EQ(tracker->getState(), initial_state);
    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityRootPublications), publications_before);
    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityPublishedDeterministicCatalogBytes), bytes_before);
}

TEST(UDTObservability, ObserverFailureUnwindsPreparedRootAndProspectiveServerReservation)
{
    const UUID database_uuid = observabilityUUID(0x8100, 3);
    auto initial_root = makeEmptyRoot(database_uuid);
    const UInt64 server_maximum
        = initial_root->getDatabaseResourceQuota().getLimits().get(ResourceLimit::LiveCatalogAndCacheBytesPerServer);
    auto tracker = ServerResourceQuotaTracker::acquireProcessTracker(server_maximum);
    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), std::move(initial_root));
    const auto initial_state = tracker->getState();

    ThrowingPublicationObserver observer;
    authority.setPublicationObserver(&observer);
    AuthorityRoot::Ptr replacement;
    {
        auto snapshot = authority.acquireCurrentRoot();
        replacement = snapshot->cloneForExactRepair();
    }

    const auto publications_before = eventValue(ProfileEvents::UDTAuthorityRootPublications);
    EXPECT_THROW(static_cast<void>(authority.preparePublication(std::move(replacement))), std::runtime_error);
    EXPECT_EQ(observer.preparations, 1);
    EXPECT_EQ(tracker->getState(), initial_state);
    EXPECT_EQ(eventValue(ProfileEvents::UDTAuthorityRootPublications), publications_before);
    authority.setPublicationObserver(nullptr);
}

TEST(UDTObservability, PolicyFreeExecutionLeavesEveryManifestCounterAtZero)
{
    const std::array manifest_events{
        ProfileEvents::UDTManifestEntries,
        ProfileEvents::UDTManifestBytes,
        ProfileEvents::UDTManifestDispatchCopies,
        ProfileEvents::UDTManifestDispatchBytes,
        ProfileEvents::UDTManifestReceiverAdmissions,
    };
    for (const auto event : manifest_events)
        ASSERT_EQ(eventValue(event), 0);

    const UUID database_uuid = observabilityUUID(0x8100, 4);
    AtomicAuthority authority(database_uuid, atomicDatabaseAuthorityCapabilities(), makeEmptyRoot(database_uuid));
    auto session = authority.beginResolutionSession();
    EXPECT_FALSE(session.findByName("missing"));
    AuthorityRoot::Ptr replacement;
    {
        auto snapshot = authority.acquireCurrentRoot();
        replacement = snapshot->cloneForExactRepair();
    }
    auto prepared = authority.preparePublication(std::move(replacement));
    authority.publish(std::move(prepared));

    for (const auto event : manifest_events)
        EXPECT_EQ(eventValue(event), 0);
}

EffectiveResourceLimits limitsWithDefinitionMaximum(UInt64 maximum)
{
    auto server = makeServerDefaultResourceLimitLayer(64ULL << 30);
    auto database = makeDatabaseDefaultResourceLimitLayer();
    database.set(ResourceLimit::DefinitionsPerDatabase, maximum);
    const std::array layers{server, database};
    return calculateEffectiveResourceLimits(layers);
}

template <typename Callback>
void expectServerQuotaError(ServerResourceQuotaTrackerError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected ServerResourceQuotaTrackerError";
    }
    catch (const ServerResourceQuotaTrackerError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TEST(UDTDatabaseQuota, OverQuotaImageAllowsNeutralAndShrinkingTransitionsButRejectsGrowth)
{
    const auto original_limits = limitsWithDefinitionMaximum(100);
    ResourceUsage usage;
    usage.set(ResourceLimit::DefinitionsPerDatabase, 10);
    auto base = DatabaseResourceQuotaTransitionBuilder::makeInitial(original_limits, usage);
    ASSERT_EQ(base->getState(), DatabaseResourceQuotaState::Active);

    const auto lowered_limits = limitsWithDefinitionMaximum(5);
    auto neutral = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, lowered_limits, {});
    ASSERT_EQ(neutral.getStatus(), DatabaseResourceQuotaPreparationStatus::Prepared);
    EXPECT_EQ(neutral.getReplacement()->getState(), DatabaseResourceQuotaState::OverQuota);
    EXPECT_EQ(neutral.getReplacement()->getUsage().get(ResourceLimit::DefinitionsPerDatabase), 10);

    ResourceDelta shrink_delta;
    shrink_delta.remove(ResourceLimit::DefinitionsPerDatabase, 5);
    auto shrink = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, lowered_limits, shrink_delta);
    ASSERT_EQ(shrink.getStatus(), DatabaseResourceQuotaPreparationStatus::Prepared);
    EXPECT_EQ(shrink.getReplacement()->getState(), DatabaseResourceQuotaState::Active);
    EXPECT_EQ(shrink.getReplacement()->getUsage().get(ResourceLimit::DefinitionsPerDatabase), 5);

    ResourceDelta growth_delta;
    growth_delta.add(ResourceLimit::DefinitionsPerDatabase, 1);
    auto growth = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, lowered_limits, growth_delta);
    EXPECT_EQ(growth.getStatus(), DatabaseResourceQuotaPreparationStatus::ResourceRejected);
    EXPECT_EQ(growth.getResourceAdmission().status, ResourceAdmissionStatus::LimitExceeded);
    ASSERT_TRUE(growth.getResourceAdmission().limit.has_value());
    EXPECT_EQ(*growth.getResourceAdmission().limit, ResourceLimit::DefinitionsPerDatabase);
}

TEST(UDTDatabaseQuota, ExactLimitInvalidRemovalOverflowAndRevisionEdgesAreAtomic)
{
    const auto limits = limitsWithDefinitionMaximum(10);
    ResourceUsage usage;
    usage.set(ResourceLimit::DefinitionsPerDatabase, 9);
    auto base = DatabaseResourceQuotaTransitionBuilder::makeInitial(limits, usage, 41);

    ResourceDelta to_limit_delta;
    to_limit_delta.add(ResourceLimit::DefinitionsPerDatabase, 1);
    auto to_limit = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, limits, to_limit_delta);
    ASSERT_EQ(to_limit.getStatus(), DatabaseResourceQuotaPreparationStatus::Prepared);
    ASSERT_NE(to_limit.getReplacement(), nullptr);
    EXPECT_EQ(to_limit.getReplacement()->getRevision(), 42);
    EXPECT_EQ(to_limit.getReplacement()->getState(), DatabaseResourceQuotaState::Active);
    EXPECT_EQ(to_limit.getReplacement()->getUsage().get(ResourceLimit::DefinitionsPerDatabase), 10);

    ResourceDelta above_limit_delta;
    above_limit_delta.add(ResourceLimit::DefinitionsPerDatabase, 2);
    auto above_limit = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, limits, above_limit_delta);
    EXPECT_EQ(above_limit.getStatus(), DatabaseResourceQuotaPreparationStatus::ResourceRejected);
    EXPECT_EQ(above_limit.getResourceAdmission().status, ResourceAdmissionStatus::LimitExceeded);
    EXPECT_EQ(base->getUsage().get(ResourceLimit::DefinitionsPerDatabase), 9);

    ResourceDelta invalid_removal_delta;
    invalid_removal_delta.remove(ResourceLimit::DefinitionsPerDatabase, 10);
    auto invalid_removal = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, limits, invalid_removal_delta);
    EXPECT_EQ(invalid_removal.getStatus(), DatabaseResourceQuotaPreparationStatus::ResourceRejected);
    EXPECT_EQ(invalid_removal.getResourceAdmission().status, ResourceAdmissionStatus::InvalidRemoval);

    ResourceDelta overflow_delta;
    overflow_delta.add(ResourceLimit::DefinitionsPerDatabase, std::numeric_limits<UInt64>::max());
    overflow_delta.add(ResourceLimit::DefinitionsPerDatabase, 1);
    auto overflow = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(base, limits, overflow_delta);
    EXPECT_EQ(overflow.getStatus(), DatabaseResourceQuotaPreparationStatus::ResourceRejected);
    EXPECT_EQ(overflow.getResourceAdmission().status, ResourceAdmissionStatus::ArithmeticOverflow);

    auto invalid_base = DatabaseResourceQuotaTransitionBuilder::prepareReplacement({}, limits, {});
    EXPECT_EQ(invalid_base.getStatus(), DatabaseResourceQuotaPreparationStatus::InvalidBase);
    auto last_revision = DatabaseResourceQuotaTransitionBuilder::makeInitial(limits, usage, std::numeric_limits<UInt64>::max());
    auto revision_overflow = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(last_revision, limits, {});
    EXPECT_EQ(revision_overflow.getStatus(), DatabaseResourceQuotaPreparationStatus::RevisionOverflow);
    EXPECT_EQ(revision_overflow.getReplacement(), nullptr);
}

TEST(UDTServerQuota, ConcurrentPrepareCommitRollbackReleaseAndPolicyConflictAreExact)
{
    constexpr UInt64 maximum_bytes = 4'096;
    constexpr UInt64 reservation_bytes = 64;
    constexpr std::size_t thread_count = 32;
    auto tracker = ServerResourceQuotaTracker::acquireProcessTracker(maximum_bytes);

    std::atomic<UInt64> committed_bytes = 0;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (std::size_t index = 0; index < thread_count; ++index)
    {
        threads.emplace_back(
            [&, index]
            {
                auto reservation = tracker->prepare(reservation_bytes);
                if (index % 2 == 0)
                {
                    reservation.commit();
                    committed_bytes.fetch_add(reservation_bytes, std::memory_order_relaxed);
                }
            });
    }
    for (auto & thread : threads)
        thread.join();
    EXPECT_EQ(tracker->getState().charged_bytes, committed_bytes.load(std::memory_order_relaxed));
    EXPECT_EQ(
        CurrentMetrics::get(CurrentMetrics::UDTLiveCatalogAndCacheBytes),
        static_cast<CurrentMetrics::Value>(committed_bytes.load(std::memory_order_relaxed)));

    EXPECT_THROW(static_cast<void>(ServerResourceQuotaTracker::acquireProcessTracker(maximum_bytes - 1)), ServerResourceQuotaTrackerError);
    EXPECT_THROW(static_cast<void>(tracker->prepare(maximum_bytes)), ServerResourceQuotaTrackerError);
    EXPECT_EQ(tracker->getState().charged_bytes, committed_bytes.load(std::memory_order_relaxed));

    threads.clear();
    const std::size_t committed_reservations = thread_count / 2;
    for (std::size_t index = 0; index < committed_reservations; ++index)
        threads.emplace_back([&] { tracker->releaseCommitted(reservation_bytes); });
    for (auto & thread : threads)
        thread.join();
    EXPECT_EQ(tracker->getState().charged_bytes, 0);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::UDTLiveCatalogAndCacheBytes), 0);
}

TEST(UDTServerQuota, ReservationMoveRollbackCommitAndExactLimitPreserveOneCharge)
{
    constexpr UInt64 maximum_bytes = 128;
    auto tracker = ServerResourceQuotaTracker::acquireProcessTracker(maximum_bytes);
    EXPECT_EQ(tracker->getState(), (ServerResourceQuotaTrackerState{.charged_bytes = 0, .maximum_bytes = maximum_bytes}));

    {
        auto zero = tracker->prepare(0);
        EXPECT_EQ(zero.getChargedBytes(), 0);
        zero.commit();
    }
    EXPECT_EQ(tracker->getState().charged_bytes, 0);

    auto first = tracker->prepare(64);
    auto replaced = tracker->prepare(32);
    EXPECT_EQ(tracker->getState().charged_bytes, 96);
    replaced = std::move(first);
    EXPECT_EQ(first.getChargedBytes(), 0);
    EXPECT_EQ(replaced.getChargedBytes(), 64);
    EXPECT_EQ(tracker->getState().charged_bytes, 64);
    replaced.commit();

    auto exact_limit = tracker->prepare(64);
    exact_limit.commit();
    EXPECT_EQ(tracker->getState().charged_bytes, maximum_bytes);
    expectServerQuotaError(ServerResourceQuotaTrackerError::Code::LimitExceeded, [&] { static_cast<void>(tracker->prepare(1)); });
    EXPECT_EQ(tracker->getState().charged_bytes, maximum_bytes);

    tracker->releaseCommitted(64);
    tracker->releaseCommitted(64);
    EXPECT_EQ(tracker->getState().charged_bytes, 0);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::UDTLiveCatalogAndCacheBytes), 0);
}

}
}
