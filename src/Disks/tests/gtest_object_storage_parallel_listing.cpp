#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageParallelListingIterator.h>

#include <atomic>
#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// A synthetic, read-only object-storage "directory tree" used to drive the parallel listing
/// iterator without talking to a real object storage. Each prefix maps to the keys directly under it
/// and to its immediate sub-"directories" (common prefixes). `listLevel` mimics `ListObjectsV2` with
/// a delimiter: it paginates the objects of a single prefix by `page_size`, returning the common
/// prefixes together with the first page.
struct FakeTree
{
    struct Node
    {
        std::vector<std::string> objects;
        std::vector<std::string> common_prefixes;
    };

    std::map<std::string, Node> nodes;
    size_t page_size = 1000;
    std::atomic<size_t> requests{0};

    ObjectStorageListResult listLevel(const std::string & prefix, const std::string & token) const
    {
        const_cast<FakeTree *>(this)->requests.fetch_add(1, std::memory_order_relaxed);

        ObjectStorageListResult result;
        auto it = nodes.find(prefix);
        if (it == nodes.end())
            return result;

        const Node & node = it->second;
        const size_t offset = token.empty() ? 0 : std::stoul(token);
        const size_t end = std::min(offset + page_size, node.objects.size());

        for (size_t i = offset; i < end; ++i)
            result.objects.push_back(std::make_shared<RelativePathWithMetadata>(node.objects[i]));

        /// Return the sub-directories together with the first page (as S3 may do).
        if (offset == 0)
            result.common_prefixes = node.common_prefixes;

        if (end < node.objects.size())
        {
            result.is_truncated = true;
            result.next_continuation_token = std::to_string(end);
        }
        return result;
    }
};

ObjectStorageParallelListingIterator::ListLevelFunction makeListLevel(const FakeTree & tree)
{
    return [&tree](const std::string & prefix, const std::string & token) { return tree.listLevel(prefix, token); };
}

/// Drains the iterator the way `GlobIterator` does and returns all produced keys.
std::vector<std::string> drain(ObjectStorageParallelListingIterator & iterator)
{
    std::vector<std::string> keys;
    while (auto batch = iterator.getCurrentBatchAndScheduleNext())
        for (const auto & object : *batch)
            keys.push_back(object->relative_path);
    return keys;
}

auto descendAll = [](const std::string &) { return true; };

}

TEST(ObjectStorageParallelListing, EmptyResult)
{
    FakeTree tree;
    tree.nodes["root/"] = {}; /// no objects, no sub-directories

    for (size_t threads : {1, 4})
    {
        ObjectStorageParallelListingIterator iterator("root/", threads, 1000, makeListLevel(tree), descendAll);
        EXPECT_TRUE(drain(iterator).empty()) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, MissingRootProducesNothing)
{
    FakeTree tree; /// "root/" is not present at all
    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, makeListLevel(tree), descendAll);
    EXPECT_TRUE(drain(iterator).empty());
}

TEST(ObjectStorageParallelListing, FilesAtRootOnly)
{
    FakeTree tree;
    tree.nodes["root/"] = {{"root/a", "root/b", "root/c"}, {}};

    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, makeListLevel(tree), descendAll);
    auto keys = drain(iterator);
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"root/a", "root/b", "root/c"}));
}

TEST(ObjectStorageParallelListing, WalksWholeTree)
{
    FakeTree tree;
    tree.nodes["root/"] = {{"root/top.txt"}, {"root/2020/", "root/2021/"}};
    tree.nodes["root/2020/"] = {{}, {"root/2020/01/", "root/2020/02/"}};
    tree.nodes["root/2021/"] = {{}, {"root/2021/01/"}};
    tree.nodes["root/2020/01/"] = {{"root/2020/01/a", "root/2020/01/b"}, {}};
    tree.nodes["root/2020/02/"] = {{"root/2020/02/a"}, {}};
    tree.nodes["root/2021/01/"] = {{"root/2021/01/a"}, {}};

    const std::vector<std::string> expected{
        "root/2020/01/a", "root/2020/01/b", "root/2020/02/a", "root/2021/01/a", "root/top.txt"};

    /// The set of produced keys must be identical regardless of the number of worker threads.
    for (size_t threads : {1, 2, 3, 8})
    {
        ObjectStorageParallelListingIterator iterator("root/", threads, 1000, makeListLevel(tree), descendAll);
        auto keys = drain(iterator);
        std::sort(keys.begin(), keys.end());
        EXPECT_EQ(keys, expected) << "threads=" << threads;
    }
}

TEST(ObjectStorageParallelListing, Pagination)
{
    FakeTree tree;
    tree.page_size = 100;

    std::vector<std::string> objects;
    for (size_t i = 0; i < 2500; ++i)
        objects.push_back("root/dir/" + std::to_string(i));

    tree.nodes["root/"] = {{}, {"root/dir/"}};
    tree.nodes["root/dir/"] = {objects, {}};

    ObjectStorageParallelListingIterator iterator("root/", 4, /* max_buffered_keys */ 256, makeListLevel(tree), descendAll);
    auto keys = drain(iterator);
    EXPECT_EQ(keys.size(), 2500u);
    std::sort(keys.begin(), keys.end());
    auto expected = objects;
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(keys, expected);
}

TEST(ObjectStorageParallelListing, Pruning)
{
    FakeTree tree;
    tree.nodes["root/"] = {{}, {"root/keep/", "root/skip/"}};
    tree.nodes["root/keep/"] = {{"root/keep/a", "root/keep/b"}, {}};
    tree.nodes["root/skip/"] = {{"root/skip/x"}, {}};

    /// Only descend into directories whose name contains "keep".
    auto should_descend = [](const std::string & prefix) { return prefix.find("keep") != std::string::npos; };

    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, makeListLevel(tree), should_descend);
    auto keys = drain(iterator);
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"root/keep/a", "root/keep/b"}));

    /// "root/skip/" must never have been listed.
    EXPECT_EQ(tree.nodes.count("root/skip/"), 1u);
}

TEST(ObjectStorageParallelListing, ExceptionPropagates)
{
    auto list_level = [](const std::string & prefix, const std::string &) -> ObjectStorageListResult
    {
        if (prefix == "root/")
        {
            ObjectStorageListResult result;
            result.common_prefixes = {"root/bad/"};
            return result;
        }
        throw std::runtime_error("listing failed");
    };

    ObjectStorageParallelListingIterator iterator("root/", 4, 1000, list_level, descendAll);
    EXPECT_THROW(drain(iterator), std::runtime_error);
}

TEST(ObjectStorageParallelListing, ConcurrencyStress)
{
    /// A wide and reasonably deep tree, listed many times with many threads, to shake out races and
    /// deadlocks. The produced key set must always be exactly the full set.
    FakeTree tree;
    std::vector<std::string> top;
    std::vector<std::string> expected;
    for (size_t d = 0; d < 32; ++d)
    {
        std::string dir = "root/d" + std::to_string(d) + "/";
        top.push_back(dir);

        std::vector<std::string> objects;
        for (size_t f = 0; f < 50; ++f)
        {
            std::string key = dir + "f" + std::to_string(f);
            objects.push_back(key);
            expected.push_back(key);
        }
        tree.nodes[dir] = {objects, {}};
    }
    tree.nodes["root/"] = {{}, top};
    tree.page_size = 16;
    std::sort(expected.begin(), expected.end());

    for (size_t attempt = 0; attempt < 25; ++attempt)
    {
        ObjectStorageParallelListingIterator iterator("root/", 8, /* max_buffered_keys */ 64, makeListLevel(tree), descendAll);
        auto keys = drain(iterator);
        std::sort(keys.begin(), keys.end());
        ASSERT_EQ(keys, expected) << "attempt=" << attempt;
    }
}
