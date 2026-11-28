#pragma once

#include <memory>

namespace DB
{

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;

/// One `INSERT` into a table, see `IStorage::beginWrite`.
class IStorageWriteSession
{
public:
    virtual ~IStorageWriteSession() = default;

    /// One sink per stream of the insert. The sinks can work in parallel.
    virtual SinkToStoragePtr createSink() = 0;

    /// Called once, after all the sinks finished without an error.
    virtual void commit() = 0;
};

using StorageWriteSessionPtr = std::shared_ptr<IStorageWriteSession>;

}
