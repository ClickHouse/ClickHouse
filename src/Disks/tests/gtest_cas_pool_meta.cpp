#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include "cas_test_helpers.h"

#include <utility>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}
}

using namespace DB::Cas;

using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::FakeClock;
using DB::Cas::tests::expectThrowsCode;

namespace
{

CasRequests makeRequests(BackendPtr backend, FakeClock & clock, Fence fence = Fence::open())
{
    return CasRequests(std::move(backend), std::move(fence), clock.nowFn(), clock.sleepFn());
}

}

TEST(CASPoolMeta, AdmitOrValidateEndsAtTheDeadlineUnderPerpetualConflict)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    Layout layout("pool");
    auto requests = makeRequests(backend, clock);

    auto create_op = requests.admit();
    const PoolMeta created = PoolMeta::createOrValidate(
        create_op, layout, /*blob_header_len=*/256, /*gc_shards=*/1,
        BlobHashAlgo::CityHash128, /*allow_new=*/false, /*allow_mint=*/true);
    EXPECT_EQ(created.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));

    /// Rig the key permanently hot: every write attempt races a concurrent rewrite of the SAME
    /// content, so the object's incarnation moves under every attempt and admission of a new algo
    /// never lands. `putOverwrite` mints a fresh incarnation even though the bytes are unchanged.
    const String key = layout.poolMetaKey();
    EXPECT_TRUE(clock.sleeps.empty());   /// nothing paced yet -- the trailing check below is about THIS call
    bool inside_hook = false;
    backend->onBeforeWrite(key, [&]
    {
        if (inside_hook)
            return;
        inside_hook = true;
        auto hook_requests = DB::Cas::tests::openRequestsForTest(BackendPtr(backend));
        auto hook_op = hook_requests.admit();
        if (auto cur = hook_op.read(key, Retry::once()))
            (void)hook_op.replace(key, cur->bytes, cur->etag, Retry::once());
        inside_hook = false;
    });

    /// `orThrow`'s `GaveUp{Deadline}` arm throws exactly `NETWORK_ERROR` (`throwCasWriteRetryLater`),
    /// pinning the deadline outcome apart from the two failures a wrong migration could also throw as
    /// a `DB::Exception` here: `LOGICAL_ERROR` (the absence branch) or `BAD_ARGUMENTS` (`allow_new`
    /// plumbing regressed).
    auto admit_op = requests.admit();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        (void)PoolMeta::createOrValidate(
            admit_op, layout, 256, /*gc_shards=*/1,
            BlobHashAlgo::XXH3_128, /*allow_new=*/true, /*allow_mint=*/false);
    });
    /// Bounded by the deadline, not a live-lock: it paced its retries rather than spinning.
    EXPECT_FALSE(clock.sleeps.empty());
}
