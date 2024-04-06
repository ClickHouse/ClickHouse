#pragma once
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/StreamQueue/StreamQueueSettings.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include "config.h"

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
        const StorageID & source_table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);
    String getName() const override { return StreamQueueStorageName; }

private:
    void startup() override;
    void shutdown(bool is_drop) override;

    void threadFunc();

    std::unique_ptr<StreamQueueSettings> settings;
    const StorageID & source_table_id;

    BackgroundSchedulePool::TaskHolder task;
    bool shutdown_called = false;

    LoggerPtr log;
};
}
