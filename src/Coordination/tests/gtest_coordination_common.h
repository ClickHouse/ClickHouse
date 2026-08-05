#pragma once

#include "config.h"

#if USE_NURAFT

#include <filesystem>

#include <Coordination/ACLMap.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>
#include <Coordination/KeeperMemNodesStorage.h>
#include <Coordination/KeeperStorage_fwd.h>
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

DB::KeeperContextPtr makeKeeperContext(bool use_lsmt_storage, std::shared_ptr<DB::CoordinationSettings> settings = nullptr);

/// Shared fixture logic. Not instantiated directly: the concrete fixtures below pick the parameter
/// set (storage implementation, optionally times compression).
class CoordinationTestBase
{
public:
    DB::KeeperContextPtr keeper_context;
    LoggerPtr log{getLogger("CoordinationTest")};
    bool use_lsmt_storage = false;

    /// A fully-operational fixture context (logs already preprocessed).
    DB::KeeperContextPtr makeKeeperContext(std::shared_ptr<DB::CoordinationSettings> settings = nullptr) const
    {
        auto context = ::makeKeeperContext(use_lsmt_storage, std::move(settings));
        context->setLocalLogsPreprocessed();
        return context;
    }

    void commonSetup()
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        const char * log_level = std::getenv("TEST_LOG_LEVEL"); // NOLINT(concurrency-mt-unsafe)
        Poco::Logger::root().setLevel(log_level ? log_level : "none");
        keeper_context = makeKeeperContext();
    }

    void setLogDirectory(const std::string & path) const { keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", path)); }

    void setSnapshotDirectory(const std::string & path) const
    {
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", path));
    }

    void setStateFileDirectory(const std::string & path) const
    {
        keeper_context->setStateFileDisk(std::make_shared<DB::DiskLocal>("StateFile", path));
    }
};

/// Parametrized by storage implementation.
class CoordinationTest : public CoordinationTestBase, public ::testing::TestWithParam</*use_lsmt_storage*/ bool>
{
    void SetUp() override
    {
        use_lsmt_storage = GetParam();
        commonSetup();
    }
};

struct StorageTypeAndCompression
{
    bool use_lsmt_storage = false;
    bool enable_compression = false;
};

/// Parametrized by storage implementation times snapshot/log compression. Use for tests that read
/// `enable_compression` / `extension`.
class CoordinationTestWithCompression : public CoordinationTestBase, public ::testing::TestWithParam<StorageTypeAndCompression>
{
public:
    bool enable_compression = false;
    std::string extension;

private:
    void SetUp() override
    {
        use_lsmt_storage = GetParam().use_lsmt_storage;
        enable_compression = GetParam().enable_compression;
        extension = enable_compression ? ".zstd" : "";
        commonSetup();
    }
};

/// Creates a committed node.
void addNode(DB::KeeperStorage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner = 0, DB::ACLId acl_id = 0, int64_t seq_num = 0);

Coordination::ACLs getUncommittedACLs(DB::KeeperStorage & storage, std::string_view path);

/// Committed-node helpers that only use the implementation-agnostic KeeperStorage API.
bool committedNodeExists(DB::KeeperStorage & storage, std::string_view path);
/// Returns "<NO NODE>" if node doesn't exist.
std::string committedNodeData(DB::KeeperStorage & storage, std::string_view path);
/// Returns "<NO NODE>" if node doesn't exist.
std::string uncommittedNodeData(DB::KeeperStorage & storage, std::string_view path);

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
