#pragma once

#include <functional>
#include <string>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

class WriteBufferFromFileBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);

    void sync() override = 0;
    virtual std::string getFileName() const = 0;

    /// Installs an operation-local cancellation check before the first write.
    virtual void setCancellationHook(std::function<void()> /* cancellation_hook */) { }
};

}
