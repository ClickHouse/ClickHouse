#include <gtest/gtest.h>
#include <Storages/ObjectStorage/StorageObjectStorageStableTaskDistributor.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

using namespace DB;

namespace
{
    class TestIterator : public IObjectIterator
    {
    public:
        explicit TestIterator(const std::vector<std::string> & paths)
        {
            for (const auto & path : paths)
                objects.push_back(std::make_shared<ObjectInfo>(path));
        }
        ~TestIterator() override = default;
        ObjectInfoPtr next(size_t) override
        {
            if (objects.empty())
                return nullptr;
            auto res = objects.front();
            objects.pop_front();
            return res;
        }
        size_t estimatedKeysCount() override
        {
            return objects.size();
        }
    private:
        std::list<ObjectInfoPtr> objects;
    };

    // Make path like '/path/file0'
    std::string makePath(size_t file_num)
    {
        const std::string common_path = "/path/file";
        return common_path + std::to_string(file_num);
    }

    std::shared_ptr<IObjectIterator> makeIterator()
    {
        std::vector<std::string> paths;
        for (size_t i = 0; i < 10; ++i)
            paths.emplace_back(makePath(i));

        return std::make_shared<TestIterator>(paths);
    }

    // Get next N objects for replica replica_id
    bool extractNForReplica(StorageObjectStorageStableTaskDistributor & distributor, std::vector<std::string> & paths, size_t replica_id, size_t n)
    {
        for (size_t i = 0; i < n; ++i)
        {
            ObjectInfoPtr object = distributor.getNextTask(replica_id);
            if (!object)
                return false;
            auto path = object->getPath();
            paths.emplace_back(path);
        }
        return true;
    }

    // Head of the list must contains all paths for files with numbers from file_nums
    bool checkHead(const std::vector<std::string> & paths, std::vector<size_t> file_nums)
    {
        if (paths.size() < file_nums.size())
            return false;

        // Order is unspecified
        std::set<std::string> response_paths;
        std::set<std::string> expected_paths;
        for (size_t i = 0; i < file_nums.size(); ++i)
        {
            response_paths.insert(paths[i]);
            expected_paths.insert(makePath(file_nums[i]));
        }

        return std::equal(response_paths.begin(), response_paths.end(), expected_paths.begin());
    }
}

/*
For specific paths '/path/file0'..'/path/file9'and nodes 'replica0'..'replicaN'
the replica priorities for the paths are the follows:
/path/file0 - 1,0,2,3
/path/file1 - 2,0,3,1
/path/file2 - 1,2,3,0
/path/file3 - 3,1,2,0
/path/file4 - 1,3,2,0
/path/file5 - 2,0,3,1
/path/file6 - 0,2,1,3
/path/file7 - 2,3,1,0
/path/file8 - 2,1,3,0
/path/file9 - 3,0,2,1
In all tests this order is checked.
*/

TEST(RendezvousHashing, SingleNode)
{
    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica0", "replica1", "replica2", "replica3"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 0, 10));
        ASSERT_TRUE(checkHead(paths, {6}));
    }

    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica0", "replica1", "replica2", "replica3"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 1, 10));
        ASSERT_TRUE(checkHead(paths, {0, 2, 4}));
    }

    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica0", "replica1", "replica2", "replica3"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 2, 10));
        ASSERT_TRUE(checkHead(paths, {1, 5, 7, 8}));
    }

    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica0", "replica1", "replica2", "replica3"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 3, 10));
        ASSERT_TRUE(checkHead(paths, {3, 9}));
    }
}

TEST(RendezvousHashing, MultipleNodes)
{
    auto iterator = makeIterator();
    std::vector<std::string> replicas = {"replica0", "replica1", "replica2", "replica3"};
    StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 0, 1));
        ASSERT_TRUE(checkHead(paths, {6}));
    }

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 1, 3));
        ASSERT_TRUE(checkHead(paths, {0, 2, 4}));
    }

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 2, 4));
        ASSERT_TRUE(checkHead(paths, {1, 5, 7, 8}));
    }

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 3, 2));
        ASSERT_TRUE(checkHead(paths, {3, 9}));
    }
}

TEST(RendezvousHashing, SingleNodeReducedCluster)
{
    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica2", "replica1"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 0, 10));
        ASSERT_TRUE(checkHead(paths, {1, 5, 6, 7, 8, 9}));
    }

    {
        auto iterator = makeIterator();
        std::vector<std::string> replicas = {"replica2", "replica1"};
        StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 1, 10));
        ASSERT_TRUE(checkHead(paths, {0, 2, 3, 4}));
    }
}

TEST(RendezvousHashing, MultipleNodesReducedCluster)
{
    auto iterator = makeIterator();
    std::vector<std::string> replicas = {"replica2", "replica1"};
    StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 0, 6));
        ASSERT_TRUE(checkHead(paths, {1, 5, 6, 7, 8, 9}));
    }

    {
        std::vector<std::string> paths;
        ASSERT_TRUE(extractNForReplica(distributor, paths, 1, 4));
        ASSERT_TRUE(checkHead(paths, {0, 2, 3, 4}));
    }
}

TEST(RendezvousHashing, MultipleNodesReducedClusterOneByOne)
{
    auto iterator = makeIterator();
    std::vector<std::string> replicas = {"replica2", "replica1"};
    StorageObjectStorageStableTaskDistributor distributor(iterator, std::move(replicas), false);

    std::vector<std::string> paths0;
    std::vector<std::string> paths1;

    // replica2 has high priority for 6 of 10 files
    // replica1 has high priority for 4 of 10 files
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths1, 1, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths1, 1, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths1, 1, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths1, 1, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));
    ASSERT_TRUE(extractNForReplica(distributor, paths0, 0, 1));

    ASSERT_TRUE(checkHead(paths0, {1, 5, 6, 7, 8, 9}));
    ASSERT_TRUE(checkHead(paths1, {0, 2, 3, 4}));
}
