#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPlainObjects.h>
#include "cas_test_helpers.h"

#include <functional>
#include <utility>

using namespace DB::Cas;

using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::FakeClock;

namespace
{

/// Every test drives `CasRequests` on an injected clock (mirrors `gtest_cas_requests.cpp`'s
/// `makeRequests`), so a policy's whole deadline is exercised in no wall-clock time.
CasRequests makeRequests(BackendPtr backend, FakeClock & clock, Fence fence = Fence::open())
{
    return CasRequests(std::move(backend), std::move(fence), clock.nowFn(), clock.sleepFn());
}

/// Refuses the FIRST removal attempt of every key with `Mismatch`, then delegates -- models a
/// concurrent replacement observed between `removeCurrent`'s internal HEAD and its DELETE.
struct MismatchOnceOnRemoveBackend : InMemoryBackend
{
    using InMemoryBackend::head;

    size_t heads = 0;
    bool refuse_next_remove = true;

    std::optional<Backend::RawMeta> head(const String & key, TransportAccess & access) override
    {
        ++heads;
        return InMemoryBackend::head(key, access);
    }

    Backend::RawRemoval remove(const String & key, const String & expected_value, TransportAccess & access) override
    {
        if (std::exchange(refuse_next_remove, false))
            return Backend::RawRemoval::Mismatch;
        return InMemoryBackend::remove(key, expected_value, access);
    }
};

}

TEST(CASPlainObjects, CasPutObjectIssuesHeadsOnly)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    Layout layout("pool");
    auto requests = makeRequests(backend, clock);
    CasPlainObjects objects(requests, layout);

    const RootNamespace ns("t");
    const auto life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.namespaceFileKey(life, "f");

    /// The create.
    objects.putNamespaceFile(life, "f", "hello");
    EXPECT_GT(backend->headCount(key), 0u);
    EXPECT_EQ(backend->getCount(key), 0u);

    /// A replace over an existing object follows the same protocol: HEAD only, never a body GET.
    objects.putNamespaceFile(life, "f", "world");
    EXPECT_EQ(backend->getCount(key), 0u);
    EXPECT_EQ(objects.getNamespaceFile(life, "f"), "world");
}

TEST(CASPlainObjects, CasRemoveObjectReheadsOnMismatch)
{
    FakeClock clock;
    auto backend = std::make_shared<MismatchOnceOnRemoveBackend>();
    Layout layout("pool");
    auto requests = makeRequests(backend, clock);
    CasPlainObjects objects(requests, layout);

    objects.putMountpointObject("f", "v");
    backend->heads = 0;

    /// The injected `Mismatch` on the first attempt must not surface as a failure: `removeCurrent`
    /// re-heads the key and retries against what it now observes.
    objects.removeMountpointObject("f");
    EXPECT_GE(backend->heads, 2u);
    EXPECT_FALSE(objects.mountpointObjectExists("f"));
}
