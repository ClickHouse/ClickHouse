#pragma once
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Storages/StreamQueue/StreamQueueSettings.h>
#include "config.h"

namespace DB
{
const String StreamQueueName = "StreamQueue";

class StorageStreamQueue : public IStorage, WithContext
{
public:
    StorageStreamQueue(
        std::unique_ptr<StreamQueueSettings> settings_,
        const StorageID & table_id_,
        ContextPtr context_);
    String getName() const override { return StreamQueueName; }
private:
    std::unique_ptr<StreamQueueSettings> settings;
};
}
