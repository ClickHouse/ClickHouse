#pragma once

#include <Common/config.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fcntl.h>
#include <string>
#include <memory>


namespace DB
{
/** Accepts NFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class WriteBufferFromNFS final : public BufferWithOwnMemory<WriteBuffer>
{

public:
    WriteBufferFromNFS(
        const String & nfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const WriteSettings write_settings_ = {},
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = O_WRONLY);

    WriteBufferFromNFS(WriteBufferFromNFS &&) = default;

    ~WriteBufferFromNFS() override;

    void nextImpl() override;

    void sync() override;

private:
    void finalizeImpl() override;
    struct WriteBufferFromNFSImpl;
    std::unique_ptr<WriteBufferFromNFSImpl> impl;
};

}

