#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <string>
#include <memory>
#include <hdfs/hdfs.h>
#include <base/types.h>
#include <Interpreters/Context.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromHDFS : public SeekableReadBufferWithSize
{
struct ReadBufferFromHDFSImpl;

public:
    ReadBufferFromHDFS(const String & hdfs_uri_, const String & hdfs_file_path_,
                       const Poco::Util::AbstractConfiguration & config_,
                       size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                       size_t read_until_position_ = 0);

    ~ReadBufferFromHDFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> getTotalSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

private:
    std::unique_ptr<ReadBufferFromHDFSImpl> impl;
};
}

#endif
