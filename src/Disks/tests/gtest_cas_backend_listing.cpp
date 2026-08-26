#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>

#include <algorithm>
#include <vector>

using namespace DB::Cas;

TEST(CASBackendListing, ForEachWalksEveryPageOnce)
{
    InMemoryBackend b;
    for (int i = 0; i < 2500; ++i)
        b.putIfAbsent("p/" + std::to_string(1000000 + i), "v");
    b.putIfAbsent("q/other", "v");   /// out of prefix — must not be visited

    std::vector<String> seen;
    forEachListedKey(b, "p/", [&](const ListedKey & k) { seen.push_back(k.key); }, /*page_limit=*/1000);
    EXPECT_EQ(seen.size(), 2500u);                                  /// paged (3 pages), no key dropped/duplicated
    EXPECT_TRUE(std::is_sorted(seen.begin(), seen.end()));
}

TEST(CASBackendListing, ForEachEmptyPrefixVisitsNothing)
{
    InMemoryBackend b;
    b.putIfAbsent("q/other", "v");

    size_t visits = 0;
    forEachListedKey(b, "p/", [&](const ListedKey &) { ++visits; });
    EXPECT_EQ(visits, 0u);
}

TEST(CASBackendListing, ClassifyMapsEveryDeleteKind)
{
    EXPECT_EQ(classifyDeleteOutcome({DeleteOutcome::Kind::Deleted, false}),       DeleteClass::Deleted);
    EXPECT_EQ(classifyDeleteOutcome({DeleteOutcome::Kind::NotFound, false}),      DeleteClass::Absent);
    EXPECT_EQ(classifyDeleteOutcome({DeleteOutcome::Kind::TokenMismatch, false}), DeleteClass::Replaced);

    EXPECT_EQ(deleteClassName(DeleteClass::Deleted),  "deleted");
    EXPECT_EQ(deleteClassName(DeleteClass::Absent),   "absent");
    EXPECT_EQ(deleteClassName(DeleteClass::Replaced), "replaced");
}
