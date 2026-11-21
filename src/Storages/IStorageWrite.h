#pragma once

#include <memory>

namespace DB
{

class SinkToStorage;
using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;

class IStorageWrite
{
public:
    virtual ~IStorageWrite() = default;

    virtual SinkToStoragePtr createSink() = 0;
    virtual void commit() = 0;
};

using StorageWritePtr = std::shared_ptr<IStorageWrite>;

}
