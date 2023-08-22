#pragma once

#include "config.h"
#include <string>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/**
 * Accepts CFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromCFS : public ReadBufferFromFileBase
{
struct ReadBufferFromCFSImpl;

public:
    ReadBufferFromCFS(
        const String & cfs_file_path_,
        const ReadSettings & settings_,
        UInt64 max_single_read_retries_ = 2,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool use_external_buffer_ = false);

    ~ReadBufferFromCFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    size_t getFileSize() override;

    // the remote file offset of buffer end.
    size_t getFileOffsetOfBufferEnd() const override;

    // the position of remote file
    off_t getPosition() override;

    void setReadUntilPosition(size_t position) override;

    String getFileName() const override;

    String getInfoForLog() override;

    bool supportsRightBoundedReads() const override { return true; }
private:
    bool use_external_buffer;
    std::unique_ptr<ReadBufferFromCFSImpl> impl;
    Poco::Logger* log;
};

}
