#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/BlobLinkCounts.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <gtest/gtest.h>

#include <map>
#include <random>

using namespace DB;

TEST(FsSnapshot, BranchesRemainIndependent)
{
    auto blob_link_counts = std::make_shared<BlobLinkCounts>();
    FsSnapshot original(blob_link_counts);
    original.recordDirectoryPath("table/part", {.remote_path = "remote", .etag = "etag", .files = {}});
    original.recordFile("table/part/data", {123, 456, ""});

    FsSnapshot renamed(original.getRoot(), blob_link_counts);
    FsSnapshot removed(original.getRoot(), blob_link_counts);
    renamed.moveDirectory("table/part", "other/part");
    removed.removeFile("table/part/data");
    removed.recordFile("table/part/new", {789, 123, ""});

    ASSERT_TRUE(original.existsFile("table/part/data"));
    EXPECT_FALSE(original.existsFile("table/part/new"));
    EXPECT_FALSE(original.existsDirectory("other"));
    EXPECT_EQ(original.getFileRemoteInfo("table/part/data")->bytes_size, 123);
    EXPECT_TRUE(renamed.existsFile("other/part/data"));
    EXPECT_FALSE(renamed.existsDirectory("table"));
    EXPECT_FALSE(removed.existsFile("table/part/data"));
    EXPECT_TRUE(removed.existsFile("table/part/new"));

    FsSnapshot before_removal(renamed.getRoot(), blob_link_counts);
    renamed.removeDirectory("other");
    EXPECT_TRUE(renamed.listDirectory("").empty());
    EXPECT_TRUE(before_removal.existsFile("other/part/data"));
    EXPECT_THROW(original.moveDirectory("table", "table/part/child"), std::exception);
    EXPECT_THROW(original.recordFile("table/part/data", {0, 0, ""}), std::exception);
    EXPECT_TRUE(original.existsFile("table/part/data"));
}

TEST(FsSnapshot, ConcurrentRemovalsOfLinksFindTheLastLink)
{
    FsMetadata fs(CurrentMetrics::end(), CurrentMetrics::end());
    std::unordered_map<std::string, DirectoryRemoteInfo> layout;
    layout["a"] = {.remote_path = "ra", .etag = "", .files = {{"data", {1, 0, "shared"}}}, .has_explicit_file_list = true};
    layout["b"] = {.remote_path = "rb", .etag = "", .files = {{"data", {1, 0, "shared"}}}, .has_explicit_file_list = true};
    fs.applyLayout(std::move(layout));

    /// Two transactions on different directories start from the same committed state, so each sees the other link left.
    auto first = fs.takeReadWriteSnapshot();
    auto second = fs.takeReadWriteSnapshot();
    first->resetToRoot(fs.takeReadOnlySnapshot()->getRoot());
    second->resetToRoot(fs.takeReadOnlySnapshot()->getRoot());
    ASSERT_EQ(first->getBlobLinkCount("shared"), 2);
    ASSERT_EQ(second->getBlobLinkCount("shared"), 2);
    first->removeFile("a/data");
    first->removeBlobLink("shared");
    second->removeFile("b/data");
    second->removeBlobLink("shared");

    EXPECT_TRUE(fs.applyJournal(first->getJournal()).empty());
    EXPECT_EQ(fs.applyJournal(second->getJournal()), std::vector<std::string>{"shared"});
    EXPECT_FALSE(fs.takeReadOnlySnapshot()->existsFile("a/data"));
    EXPECT_FALSE(fs.takeReadOnlySnapshot()->existsFile("b/data"));
}

TEST(FsSnapshot, WideDirectorySharesUnchangedEntries)
{
    auto blob_link_counts = std::make_shared<BlobLinkCounts>();
    FsSnapshot original(blob_link_counts);
    for (size_t i = 0; i < 10000; ++i)
        original.recordDirectoryPath("table/" + std::to_string(i), {.remote_path = std::to_string(i), .etag = "", .files = {}});

    auto children = original.getRoot()->subdirectories.findChild("table")->subdirectories;
    FsSnapshot changed(original.getRoot(), blob_link_counts);
    changed.recordFile("table/5000/data", {123, 456, ""});

    size_t copied_entries = 0;
    children.forEachChild([&](const auto &, const auto & child)
    {
        if (child.use_count() > 1)
            ++copied_entries;
    });
    /// One update must share siblings, not copy the whole directory map.
    EXPECT_LT(copied_entries, 32);
    EXPECT_FALSE(original.existsFile("table/5000/data"));
    EXPECT_TRUE(changed.existsFile("table/5000/data"));
    EXPECT_EQ(original.listDirectory("table").size(), 10000);
    EXPECT_EQ(changed.listDirectory("table").size(), 10000);
}

TEST(FsSnapshot, WideDirectoryRemovalSharesUnchangedEntries)
{
    auto blob_link_counts = std::make_shared<BlobLinkCounts>();
    FsSnapshot original(blob_link_counts);
    for (size_t i = 0; i < 10000; ++i)
        original.recordDirectoryPath("table/" + std::to_string(i), {.remote_path = std::to_string(i), .etag = "", .files = {}});

    auto children = original.getRoot()->subdirectories.findChild("table")->subdirectories;
    FsSnapshot changed(original.getRoot(), blob_link_counts);
    changed.removeDirectory("table/5000");

    size_t copied_entries = 0;
    children.forEachChild([&](const auto &, const auto & child)
    {
        if (child.use_count() > 1)
            ++copied_entries;
    });
    /// Removing one child must share siblings, just like updating one child.
    EXPECT_LT(copied_entries, 32);
    EXPECT_TRUE(original.existsDirectory("table/5000"));
    EXPECT_FALSE(changed.existsDirectory("table/5000"));
    EXPECT_EQ(original.listDirectory("table").size(), 10000);
    EXPECT_EQ(changed.listDirectory("table").size(), 9999);
}

TEST(FsSnapshot, DirectoryMapMatchesOrderedMap)
{
    FsDirectoryEntries actual;
    std::map<std::string, std::shared_ptr<FsNode>> expected;
    std::mt19937 random(123); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test

    auto check = [](const auto & map, const auto & reference)
    {
        std::map<std::string, std::shared_ptr<FsNode>> entries;
        map.forEachChild([&](const auto & name, const auto & value) { entries.emplace(name, value); });
        EXPECT_EQ(entries, reference);
        EXPECT_EQ(map.isEmpty(), reference.empty());
        for (const auto & [name, value] : reference)
            EXPECT_EQ(map.findChild(name), value);
    };

    for (size_t i = 0; i < 10000; ++i)
    {
        const auto snapshot = actual;
        const auto before = expected;
        const auto name = std::to_string(random() % 128);
        if (random() % 2)
        {
            auto value = std::make_shared<FsNode>();
            actual.putChild(name, value);
            expected[name] = value;
        }
        else
        {
            actual.removeChild(name);
            expected.erase(name);
            EXPECT_FALSE(actual.findChild(name));
        }
        check(actual, expected);
        check(snapshot, before);
    }

    while (!expected.empty())
    {
        actual.removeChild(expected.begin()->first);
        expected.erase(expected.begin());
        check(actual, expected);
    }
}
