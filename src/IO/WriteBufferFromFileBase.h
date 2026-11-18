#pragma once

#include <optional>
#include <string>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/WriteOutcome.h>

namespace DB
{

class WriteBufferFromFileBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);

    void sync() override = 0;
    virtual std::string getFileName() const = 0;

    /// The outcome of the write, for a storage that tracks it (S3). Read it after `finalize`, also after an exception.
    /// Empty for a storage that does not track it.
    virtual std::optional<WriteOutcome> getWriteOutcome() const { return std::nullopt; }
};

}
