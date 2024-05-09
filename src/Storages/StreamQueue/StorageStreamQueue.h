#pragma once
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/StreamQueue/StreamQueueSettings.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace
{
const String StreamQueueStorageName = "StreamQueue";
const String StreamQueueArgumentName = "source_table_name";
}

class StorageStreamQueue : public IStorage, WithContext
{
public:
    StorageStreamQueue(
        std::unique_ptr<StreamQueueSettings> settings_,
        const StorageID & table_id_,
        ContextPtr context_,
        StorageID source_table_id_,
        std::shared_ptr<ASTIdentifier> key_column_,
        const Names & column_names_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);
    String getName() const override { return StreamQueueStorageName; }

    zkutil::ZooKeeperPtr getZooKeeper() const;

private:
    void startup() override;
    void shutdown(bool is_drop) override;

    std::unordered_set<int64_t> readSetOfKeys();
    void writeSetOfKeys(std::unordered_set<int64_t>);

    bool createZooKeeperNode();

    void threadFunc();
    void move_data();

    std::unique_ptr<StreamQueueSettings> settings;
    StorageID stream_table_id;
    StorageID source_table_id;

    Names column_names;
    std::shared_ptr<ASTIdentifier> key_column;

    BackgroundSchedulePool::TaskHolder task;
    bool shutdown_called = false;

    bool downloading = false;

    std::string keeper_path;
    std::string keeper_key_path;

    LoggerPtr log;
};
}
