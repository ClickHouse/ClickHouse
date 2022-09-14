#pragma once

#include <boost/noncopyable.hpp>

#include <Interpreters/Context.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IVolume.h>

namespace DB
{

class TemporaryDataOnDisk;
using TemporaryDataOnDiskPtr = std::unique_ptr<TemporaryDataOnDisk>;

class TemporaryFileStream;
using TemporaryFileStreamHolder = std::unique_ptr<TemporaryFileStream>;


/// Holds set of temporary files on disk and account amount of written data.
/// If limit is set, throws exception if limit is exceeded.
/// Data can be nested, so parent account all data written by children.
/// New file stream is created with `createStream`.
/// Streams are owned by this object and will be deleted when it is deleted.
class TemporaryDataOnDisk : boost::noncopyable
{

friend class TemporaryFileStream;

public:
    struct Stat
    {
        std::atomic<size_t> compressed_size;
        std::atomic<size_t> uncompressed_size;
    };

    explicit TemporaryDataOnDisk(VolumePtr volume_, size_t limit_)
        : volume(std::move(volume_)), limit(limit_)
    {}

    explicit TemporaryDataOnDisk(std::shared_ptr<TemporaryDataOnDisk> parent_, size_t limit_)
        : parent(std::move(parent_)), volume(parent->volume), limit(limit_)
    {}

    TemporaryFileStream & createStream(const Block & header, CurrentMetrics::Value metric_scope, size_t reserve_size = 0);
    std::vector<TemporaryFileStreamHolder> & getStreams() { return streams; }

    const Stat & getStat() const { return stat; }

    /// TODO: remove
    /// Refactor all code that uses volume directly to use TemporaryDataOnDisk.
    VolumePtr getVolume() const { return volume; }

protected:
    void deltaAlloc(int compressed_size, int uncompressed_size);

    std::shared_ptr<TemporaryDataOnDisk> parent = nullptr;
    VolumePtr volume;

    std::mutex mutex; /// Protects streams
    std::vector<TemporaryFileStreamHolder> streams;

    Stat stat;
    size_t limit = 0;
};

/// Data can be written into this stream and then read.
/// After finish writing, call `finishWriting` and then `read` to read the data.
/// It's a leaf node in temporary data tree described above.
class TemporaryFileStream : boost::noncopyable
{
public:
    using Stat = TemporaryDataOnDisk::Stat;

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header, TemporaryDataOnDisk * parent_);

    void write(const Block & block);
    Stat finishWriting();
    bool isWriteFinished() const;

    Block read();

    const String & path() const { return file->getPath(); }
    Block getHeader() const;

    ~TemporaryFileStream();

private:
    void updateAlloc(size_t rows_added);

    TemporaryDataOnDisk * parent;

    size_t compressed_size = 0;
    size_t uncompressed_size = 0;
    size_t written_rows = 0;

    TemporaryFileOnDiskHolder file;

    struct OutputWriter;
    std::unique_ptr<OutputWriter> out_writer;

    struct InputReader;
    std::unique_ptr<InputReader> in_reader;
};

}
