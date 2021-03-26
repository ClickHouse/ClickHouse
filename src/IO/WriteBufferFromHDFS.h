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
class WriteBufferFromHDFS : public BufferWithOwnMemory<WriteBuffer>
{
    struct WriteBufferFromHDFSImpl;
    std::unique_ptr<WriteBufferFromHDFSImpl> impl;
public:
    WriteBufferFromHDFS(const std::string & hdfs_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    WriteBufferFromHDFS(WriteBufferFromHDFS &&) = default;

    void nextImpl() override;

    ~WriteBufferFromHDFS() override;

    void sync() override;
};
}
#endif
