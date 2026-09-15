#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include "cas_test_helpers.h"

#include <utility>

using namespace DB::Cas;

using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::FakeClock;

namespace
{

CasRequests makeRequests(BackendPtr backend, FakeClock & clock, Fence fence = Fence::open())
{
    return CasRequests(std::move(backend), std::move(fence), clock.nowFn(), clock.sleepFn());
}

}

TEST(CASRefProtocol, CrossEpochFromSealShortCircuitsWithoutAnyRequest)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    Layout layout("pool");
    auto requests = makeRequests(backend, clock);
    auto op = requests.admit();

    const RootNamespace ns("t");
    const auto life = DB::Cas::tests::fixture::fixtureLife(ns);

    /// `from_seal == RefTxnId{}`: nothing consumed yet, so there is no seal to cross from -- proved
    /// without reading anything.
    {
        const EpochCrossResult r = crossEpochFromSeal(
            op, layout, ns, RefTxnId{}, std::nullopt, RefTxnId{2, 1}, life);
        EXPECT_EQ(r.outcome, EpochCrossOutcome::NothingConsumed);
    }

    /// The caller already decoded the record at `from_seal` and knows it is not an `EpochSeal` --
    /// also proved without any read here.
    {
        const EpochCrossResult r = crossEpochFromSeal(
            op, layout, ns, RefTxnId{1, 5}, /*seal_proven=*/false, RefTxnId{2, 1}, life);
        EXPECT_EQ(r.outcome, EpochCrossOutcome::NotASeal);
    }

    EXPECT_EQ(backend->getTotal(), 0u);
}
