#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <gtest/gtest.h>

#include <map>
#include <random>

using namespace DB;

TEST(FsSnapshot, BranchesRemainIndependent)
{
    FsSnapshot original;
    original.recordDirectoryPath("table/part", {.remote_path = "remote", .etag = "etag", .files = {}});
    original.recordFile("table/part/data", {123, 456});

    FsSnapshot renamed(original.getRoot());
    FsSnapshot removed(original.getRoot());
    renamed.moveDirectory("table/part", "other/part");
    removed.removeFile("table/part/data");
    removed.recordFile("table/part/new", {789, 123});

    ASSERT_TRUE(original.existsFile("table/part/data"));
    EXPECT_FALSE(original.existsFile("table/part/new"));
    EXPECT_FALSE(original.existsDirectory("other"));
    EXPECT_EQ(original.getFileRemoteInfo("table/part/data")->bytes_size, 123);
    EXPECT_TRUE(renamed.existsFile("other/part/data"));
    EXPECT_FALSE(renamed.existsDirectory("table"));
    EXPECT_FALSE(removed.existsFile("table/part/data"));
    EXPECT_TRUE(removed.existsFile("table/part/new"));

    FsSnapshot before_removal(renamed.getRoot());
    renamed.removeDirectory("other");
    EXPECT_TRUE(renamed.listDirectory("").empty());
    EXPECT_TRUE(before_removal.existsFile("other/part/data"));
    EXPECT_THROW(original.moveDirectory("table", "table/part/child"), std::exception);
    EXPECT_THROW(original.recordFile("table/part/data", {0, 0}), std::exception);
    EXPECT_TRUE(original.existsFile("table/part/data"));
}

TEST(FsSnapshot, WideDirectorySharesUnchangedEntries)
{
    FsSnapshot original;
    for (size_t i = 0; i < 10000; ++i)
        original.recordDirectoryPath("table/" + std::to_string(i), {.remote_path = std::to_string(i), .etag = "", .files = {}});

    auto children = original.getRoot()->subdirectories.get("table")->subdirectories;
    FsSnapshot changed(original.getRoot());
    changed.recordFile("table/5000/data", {123, 456});

    size_t copied_entries = 0;
    children.forEach([&](const auto &, const auto & child)
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

TEST(FsSnapshot, DirectoryMapMatchesOrderedMap)
{
    FsDirectoryMap actual;
    std::map<std::string, std::shared_ptr<FsNode>> expected;
    std::mt19937 random(123);

    auto check = [](const auto & map, const auto & reference)
    {
        std::map<std::string, std::shared_ptr<FsNode>> entries;
        map.forEach([&](const auto & name, const auto & value) { entries.emplace(name, value); });
        EXPECT_EQ(entries, reference);
        EXPECT_EQ(map.empty(), reference.empty());
        for (const auto & [name, value] : reference)
            EXPECT_EQ(map.get(name), value);
    };

    for (size_t i = 0; i < 10000; ++i)
    {
        const auto snapshot = actual;
        const auto before = expected;
        const auto name = std::to_string(random() % 128);
        if (random() % 2)
        {
            auto value = std::make_shared<FsNode>();
            actual.set(name, value);
            expected[name] = value;
        }
        else
        {
            actual.erase(name);
            expected.erase(name);
            EXPECT_FALSE(actual.get(name));
        }
        check(actual, expected);
        check(snapshot, before);
    }

    while (!expected.empty())
    {
        actual.erase(expected.begin()->first);
        expected.erase(expected.begin());
        check(actual, expected);
    }
}
