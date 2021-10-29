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

public:
    WriteBufferFromHDFS(
        const String & hdfs_name_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = O_WRONLY);

    WriteBufferFromHDFS(WriteBufferFromHDFS &&) = default;

    ~WriteBufferFromHDFS() override;

    void nextImpl() override;

    void sync() override;

    void finalize() override;

private:
    struct WriteBufferFromHDFSImpl;
    std::unique_ptr<WriteBufferFromHDFSImpl> impl;
};

}
#endif
