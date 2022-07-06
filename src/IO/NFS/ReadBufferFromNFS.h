#pragma once

#include <Common/config.h>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/Context.h>
#include <string>
#include <memory>
#include <base/types.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/** Accepts DFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromNFS : public ReadBufferFromFileBase
{
struct ReadBufferFromNFSImpl;

public:
    ReadBufferFromNFS(
        const String & nfs_file_path_,
        const ReadSettings & settings_,
        UInt64 max_single_read_retries_ = 2,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        size_t read_until_position_ = 0);

    ~ReadBufferFromNFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    std::optional<size_t> getTotalSize();

    // the remote file offset of buffer end.
    size_t getFileOffsetOfBufferEnd() const override;

    // the position of remote file
    off_t getPosition() override;

    void setReadUntilPosition(size_t position) override;

    SeekableReadBuffer::Range getRemainingReadRange() const override;

    String getFileName() const override;

    String getInfoForLog() override;
private:
    std::unique_ptr<ReadBufferFromNFSImpl> impl;
    Poco::Logger* log;
};

}

