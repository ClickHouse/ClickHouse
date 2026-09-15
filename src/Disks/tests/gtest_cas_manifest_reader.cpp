#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasManifestReader.h>
#include "cas_test_helpers.h"

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
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

TEST(CASManifestReader, MissingManifestThrowsFileDoesntExist)
{
    FakeClock clock;
    auto backend = std::make_shared<CountingBackend>();
    Layout layout("pool");
    PoolMeta meta;
    CasEventSink sink;
    auto requests = makeRequests(backend, clock);
    CasManifestReader reader(requests, layout, meta, sink, /*manifest_decode_cache_bytes=*/0);

    const ManifestId id{RootNamespace("t"), ManifestRef{1, 1, 1}};

    /// A live ref naming a missing manifest body is INV-NO-DANGLE: never a substituted empty
    /// manifest, always the fail-closed exception -- and this must hold over the migrated
    /// `CasOperation`-based read exactly as it did over the raw backend call.
    expectThrowsCode(DB::ErrorCodes::FILE_DOESNT_EXIST, [&] { (void)reader.readManifest(id); });
    EXPECT_EQ(backend->getCount(layout.manifestKey(id)), 1u);
}
