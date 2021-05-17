#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>

namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class WriteBufferFromHDFS final : public BufferWithOwnMemory<WriteBuffer>
{
    struct WriteBufferFromHDFSImpl;
    std::unique_ptr<WriteBufferFromHDFSImpl> impl;

public:
    WriteBufferFromHDFS(const std::string & hdfs_name_, const Poco::Util::AbstractConfiguration &, size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    WriteBufferFromHDFS(WriteBufferFromHDFS &&) = default;

    ~WriteBufferFromHDFS() override;

    void nextImpl() override;

    void sync() override;

    void finalize() override;
};
}
#endif
