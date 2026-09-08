#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(StoredObject, GetTotalSizeSumsKnownSizes)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 10);
    objects.emplace_back("b", "", 20);
    objects.emplace_back("c", "", 0); /// real empty file, not "unknown"
    EXPECT_EQ(getTotalSize(objects), 30u);
}

TEST(StoredObject, GetTotalSizePropagatesUnknownSize)
{
    /// If any element is `UnknownSize`, the sum cannot be expressed — return
    /// the sentinel instead of overflowing to `~uint64_t::max` via a literal
    /// add. Callers that need an absolute byte count check for this sentinel
    /// explicitly (`ReadPipeline` buffer sizing, etc.).
    StoredObjects objects;
    objects.emplace_back("a", "", 10);
    objects.emplace_back("b", "", StoredObject::UnknownSize);
    objects.emplace_back("c", "", 20);
    EXPECT_EQ(getTotalSize(objects), StoredObject::UnknownSize);
}

TEST(StoredObject, GetTotalSizeEmptyIsZero)
{
    EXPECT_EQ(getTotalSize(StoredObjects{}), 0u);
}
