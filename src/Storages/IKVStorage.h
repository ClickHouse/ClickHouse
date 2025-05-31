#pragma once

#include <Storages/IStorage.h>

namespace DB
{
    /// Interface for key-value storage.
    /// It's user in RedisHandler

    class IKVStorage : public IStorage
    {
    public:
        using IStorage::IStorage;
    };
}
