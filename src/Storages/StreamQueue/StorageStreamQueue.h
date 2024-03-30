#pragma once
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Storages/StreamQueue/StreamQueueSettings.h>
#include <Common/logger_useful.h>
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
        const StorageID &  source_table_id_);
    String getName() const override { return StreamQueueStorageName; }
private:
    std::unique_ptr<StreamQueueSettings> settings;
    const StorageID & source_table_id;
    LoggerPtr log;
};
}
