#pragma once

#include "config.h"

#if USE_HDFS
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fcntl.h>
#include <string>
#include <memory>


namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class WriteBufferFromHDFS final : public WriteBufferFromFileBase
{

public:
    WriteBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        int replication_,
        const WriteSettings & write_settings_ = {},
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = O_WRONLY);

    ~WriteBufferFromHDFS() override;

    void nextImpl() override;

    void sync() override;

    std::string getFileName() const override { return filename; }

private:
    struct WriteBufferFromHDFSImpl;
    std::unique_ptr<WriteBufferFromHDFSImpl> impl;
    const std::string filename;
};

}
#endif
