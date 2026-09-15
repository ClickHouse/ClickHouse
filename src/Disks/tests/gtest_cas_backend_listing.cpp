#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>

#include "cas_test_helpers.h"

#include <algorithm>
#include <vector>

using namespace DB::Cas;

using DB::Cas::tests::openRequestsForTest;

TEST(CASBackendListing, ForEachWalksEveryPageOnce)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    for (int i = 0; i < 2500; ++i)
        op.create("p/" + std::to_string(1000000 + i), "v", Retry::once());
    op.create("q/other", "v", Retry::once());   /// out of prefix — must not be visited

    std::vector<String> seen;
    op.forEachListedKey("p/", [&](const ListedKey & k) { seen.push_back(k.key); return true; },
                        Retry::standard(), /*page_limit=*/1000);
    EXPECT_EQ(seen.size(), 2500u);                                  /// paged (3 pages), no key dropped/duplicated
    EXPECT_TRUE(std::is_sorted(seen.begin(), seen.end()));
}

TEST(CASBackendListing, ForEachEmptyPrefixVisitsNothing)
{
    InMemoryBackend b;
    CasRequests requests = openRequestsForTest(b);
    CasOperation op = requests.admit();
    op.create("q/other", "v", Retry::once());

    size_t visits = 0;
    op.forEachListedKey("p/", [&](const ListedKey &) { ++visits; return true; }, Retry::standard());
    EXPECT_EQ(visits, 0u);
}

/// `ClassifyMapsEveryDeleteKind` is deleted here: its whole subject was `classifyDeleteOutcome` and
/// `deleteClassName`, free helpers that translated the legacy `DeleteOutcome::Kind` three-value shape
/// into a `DeleteClass`. Both the legacy shape and the helpers are gone -- `CasOperation::remove`
/// already reports its outcome as the four-value `Removal` enum directly, with no separate
/// classification step to pin.
