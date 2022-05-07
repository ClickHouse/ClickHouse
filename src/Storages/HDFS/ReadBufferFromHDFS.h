#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <string>
#include <memory>

#include <hdfs/hdfs.h>

#include <base/types.h>
#include <Common/RangeGenerator.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Context.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileName.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ParallelReadBuffer.h>


namespace DB
{
/** Accepts HDFS path to file and opens it.
 * Closes file by himself (thus "owns" a file descriptor).
 */
class ReadBufferFromHDFS : public SeekableReadBuffer, public WithFileName, public WithFileSize
{
struct ReadBufferFromHDFSImpl;

public:
    ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        size_t file_offset_ = 0,
        size_t read_until_position_ = 0);

    ~ReadBufferFromHDFS() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> getFileSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

    String getFileName() const override;

    Range getRemainingReadRange() const override;

private:
    std::unique_ptr<ReadBufferFromHDFSImpl> impl;
};


/// Creates separate ReadBufferFromHDFS for sequence of ranges of particular HDFS file
class ReadBufferHDFSFactory : public ParallelReadBuffer::ReadBufferFactory, public WithFileName
{
public:
    explicit ReadBufferHDFSFactory(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const ReadSettings & read_settings_,
        size_t range_step_,
        size_t file_size_)
        : hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , config(config_)
        , read_settings(read_settings_)
        , range_generator(file_size_, range_step_)
        , range_step(range_step_)
        , file_size(file_size_)
    {
        assert(range_step > 0);
        assert(range_step < file_size);
    }

    SeekableReadBufferPtr getReader() override;

    off_t seek(off_t off, [[maybe_unused]] int whence) override;

    std::optional<size_t> getFileSize() override;

    String getFileName() const override
    {
        return endsWith(hdfs_uri, "/") ? hdfs_uri.substr(0, hdfs_uri.size() - 1) + hdfs_file_path : hdfs_uri + hdfs_file_path;
    }

private:
    const String hdfs_uri;
    const String hdfs_file_path;
    const Poco::Util::AbstractConfiguration & config;
    const ReadSettings read_settings;
    RangeGenerator range_generator;
    size_t range_step;
    size_t file_size;
};

}

#endif
