#pragma once

#include "config.h"

#if USE_NURAFT

#include <filesystem>

#include <Coordination/ACLMap.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>
#include <Coordination/KeeperCommon.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Disks/DiskLocal.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

#include <libnuraft/log_entry.hxx>
#include <libnuraft/log_store.hxx>

#include <gtest/gtest.h>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsNonZeroUInt64 rotate_log_storage_interval;
    extern const CoordinationSettingsUInt64 reserved_log_items;
    extern const CoordinationSettingsUInt64 snapshot_distance;
}

namespace fs = std::filesystem;
struct ChangelogDirTest
{
    std::string path;
    bool drop;
    explicit ChangelogDirTest(std::string path_, bool drop_ = true) : path(path_), drop(drop_)
    {
        EXPECT_FALSE(fs::exists(path)) << "Path " << path << " already exists, remove it to run test";
        fs::create_directory(path);
    }

    ~ChangelogDirTest()
    {
        if (fs::exists(path) && drop)
            fs::remove_all(path);
    }
};

/// TODO: Try to remove the TStorage template and make all tests use virtual methods of KeeperStorage instead.
template <typename TStorage, bool enable_compression_param>
struct TestParam
{
    using Storage = TStorage;
    static constexpr bool enable_compression = enable_compression_param;
};

template<typename TestType>
class CoordinationTest : public ::testing::Test
{
public:
    using Storage = typename TestType::Storage;
    static constexpr bool enable_compression = TestType::enable_compression;
    std::string extension;

    DB::KeeperContextPtr keeper_context;
    LoggerPtr log{getLogger("CoordinationTest")};

    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        const char * log_level = std::getenv("TEST_LOG_LEVEL"); // NOLINT(concurrency-mt-unsafe)
        Poco::Logger::root().setLevel(log_level ? log_level : "none");

        auto settings = std::make_shared<DB::CoordinationSettings>();
        keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        keeper_context->setLocalLogsPreprocessed();
        extension = enable_compression ? ".zstd" : "";
    }

    void setLogDirectory(const std::string & path) { keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", path)); }

    void setSnapshotDirectory(const std::string & path)
    {
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", path));
    }

    void setStateFileDirectory(const std::string & path)
    {
        keeper_context->setStateFileDisk(std::make_shared<DB::DiskLocal>("StateFile", path));
    }
};

/// Creates a committed node.
inline void addNode(DB::KeeperStorage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner = 0, DB::ACLId acl_id = 0)
{
    DB::KeeperNodeStats stats;
    if (ephemeral_owner)
        stats.setEphemeralOwner(ephemeral_owner);
    stats.acl_id = acl_id;
    storage.addSystemNodeIfNotExists(path, stats, data, /*update_parent_num_children=*/true, /*out_digest=*/nullptr);
}

/// Reads from the uncommitted state, which is implementation-specific, so this is templated by the
/// storage type (similar to the request handlers in KeeperStorageImpl.cpp).
template <typename Storage>
Coordination::ACLs getUncommittedACLs(Storage & storage, std::string_view path)
{
    const auto * node = storage.getUncommittedNode(path).get();
    Coordination::ACLId acl_id = node ? node->stats.acl_id : 0;
    return storage.acl_map.convertNumber(acl_id);
}

/// Committed-node helpers that only use the implementation-agnostic KeeperStorage API.
inline bool committedNodeExists(DB::KeeperStorage & storage, std::string_view path)
{
    return storage.getCommittedNodeSlow(path);
}

inline std::string committedNodeData(DB::KeeperStorage & storage, std::string_view path)
{
    std::string data;
    EXPECT_TRUE(storage.getCommittedNodeSlow(path, /*out_stats=*/nullptr, &data));
    return data;
}

using Implementation = testing::Types<TestParam<DB::KeeperStorageImpl, true>, TestParam<DB::KeeperStorageImpl, false>>;
TYPED_TEST_SUITE(CoordinationTest, Implementation);

using LogEntryPtr = nuraft::ptr<nuraft::log_entry>;

LogEntryPtr getLogEntry(const std::string & s, size_t term);

void waitDurableLogs(nuraft::log_store & log_store);

void assertFileDeleted(std::string path);

nuraft::ptr<nuraft::log_entry>
getLogEntryFromZKRequest(size_t term, int64_t session_id, int64_t zxid, const Coordination::ZooKeeperRequestPtr & request);

template <typename ResponseType>
ResponseType getSingleResponse(const auto & responses)
{
    EXPECT_FALSE(responses.empty());
    return dynamic_cast<ResponseType &>(*responses[0].response);
}

#endif
