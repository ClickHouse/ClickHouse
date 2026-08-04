#pragma once

#include <optional>
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

    /// The object-storage ETag/token the write produced, if any (e.g. the S3 PutObject /
    /// CompleteMultipartUpload response ETag). Empty for backends that do not return a write-time
    /// ETag (local files, etc.). Valid only after a successful finalize(). Lets content-addressed
    /// callers record the just-written incarnation's token WITHOUT a follow-up HEAD.
    virtual std::optional<std::string> getResultObjectETag() const { return {}; }
};

}
