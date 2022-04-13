#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <string>
#include <memory>

#include <hdfs/hdfs.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/AsynchronousReader.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/Context.h>

namespace DB
{

class AsynchronousReadBufferFromHDFS : public SeekableReadBufferWithSize
{
class AsynchronousReadBufferFromHDFSImpl;

public:
    AsynchronousReadBufferFromHDFS(const String & hdfs_uri_, const String & hdfs_file_path_,
                       const Poco::Util::AbstractConfiguration & config_,
                       size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                       size_t read_until_position_ = 0);

    ~AsynchronousReadBufferFromHDFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> getTotalSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

private:
    std::unique_ptr<AsynchronousReadBufferFromHDFSImpl> impl;
};

}
#endif
