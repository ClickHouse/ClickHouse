#include <Storages/StorageFactory.h>
#include <Storages/StreamQueue/StorageStreamQueue.h>

namespace DB
{

StorageStreamQueue::StorageStreamQueue(std::unique_ptr<StreamQueueSettings> settings_, const StorageID & table_id_, ContextPtr context_)
    : IStorage(table_id_), WithContext(context_), settings(std::move(settings_))
{
}

StoragePtr createStorage(const StorageFactory::Arguments & args)
{
    auto settings = std::make_unique<StreamQueueSettings>();
    if (args.storage_def->settings) {
        settings->loadFromQuery(*args.storage_def);
    }
    return std::make_shared<StorageStreamQueue>(
                std::move(settings),
                args.table_id,
                args.getContext());
}

void registerStorageStreamQueue(StorageFactory & factory)
{
    factory.registerStorage(
        StreamQueueName,
        createStorage,
        {
            .supports_settings = true,
            .supports_schema_inference = true,
        });
}
};
