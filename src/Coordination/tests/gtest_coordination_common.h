#pragma once

#include "config.h"

#if USE_NURAFT

#include <filesystem>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage_fwd.h>
#include <Coordination/KeeperCommon.h>

#include <Disks/DiskLocal.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

#include <libnuraft/log_entry.hxx>
#include <libnuraft/log_store.hxx>

#include <gtest/gtest.h>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool experimental_use_rocksdb;
    extern const CoordinationSettingsUInt64 rotate_log_storage_interval;
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

struct CompressionParam
{
    bool enable_compression;
    std::string extension;
};

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
        Poco::Logger::root().setLevel("trace");

        auto settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
        (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = std::is_same_v<Storage, DB::KeeperRocksStorage>;
#else
        (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = 0;
#endif
        keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        keeper_context->setLocalLogsPreprocessed();
        keeper_context->setRocksDBOptions();
        extension = enable_compression ? ".zstd" : "";
    }

    void setLogDirectory(const std::string & path) { keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", path)); }

    void setSnapshotDirectory(const std::string & path)
    {
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", path));
    }

    void setRocksDBDirectory(const std::string & path)
    {
        keeper_context->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", path));
    }

    void setStateFileDirectory(const std::string & path)
    {
        keeper_context->setStateFileDisk(std::make_shared<DB::DiskLocal>("StateFile", path));
    }
};

template <typename Storage>
void addNode(Storage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner = 0, uint64_t acl_id = 0)
{
    using Node = typename Storage::Node;
    Node node{};
    node.setData(data);
    if (ephemeral_owner)
        node.stats.setEphemeralOwner(ephemeral_owner);
    node.acl_id = acl_id;
    storage.container.insertOrReplace(path, node);
    auto child_it = storage.container.find(path);
    auto child_path = DB::getBaseNodeName(child_it->key);
    storage.container.updateValue(
        DB::parentNodePath(StringRef{path}),
        [&](auto & parent)
        {
            parent.addChild(child_path);
            parent.stats.increaseNumChildren();
        });
}

using Implementation = testing::Types<TestParam<DB::KeeperMemoryStorage, true>
                                      ,TestParam<DB::KeeperMemoryStorage, false>
#if USE_ROCKSDB
                                      ,TestParam<DB::KeeperRocksStorage, true>
                                      ,TestParam<DB::KeeperRocksStorage, false>
#endif
                                      >;
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
