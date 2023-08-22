#pragma once

#include "config.h"
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <fcntl.h>
#include <string>
#include <memory>


namespace DB
{
/**
 * Accepts CFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class WriteBufferFromCFS final :  public WriteBufferFromFileBase
{

public:
    WriteBufferFromCFS(
        const String & cfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const WriteSettings write_settings_ = {},
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = O_WRONLY);

    WriteBufferFromCFS(WriteBufferFromCFS &&) = default;

    ~WriteBufferFromCFS() override;

    void nextImpl() override;

    void sync() override;

    std::string getFileName() const override { return file_name; }
private:
    void finalizeImpl() override;
    struct WriteBufferFromCFSImpl;
    std::unique_ptr<WriteBufferFromCFSImpl> impl;
    const std::string file_name;
};

}
