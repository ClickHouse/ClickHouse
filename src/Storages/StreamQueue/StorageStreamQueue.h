#pragma once
#include <Core/BackgroundSchedulePool.h>
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
        const Names & column_names_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);
    String getName() const override { return StreamQueueStorageName; }

private:
    void startup() override;
    void shutdown(bool is_drop) override;

    void threadFunc();
    void move_data();

    std::unique_ptr<StreamQueueSettings> settings;
    StorageID source_table_id;

    Names column_names;
    String key_column;

    BackgroundSchedulePool::TaskHolder task;
    bool shutdown_called = false;

    LoggerPtr log;
};
}
