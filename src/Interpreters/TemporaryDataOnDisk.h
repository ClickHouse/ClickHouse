#pragma once

#include <boost/noncopyable.hpp>

#include <Interpreters/Context.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IVolume.h>

namespace DB
{

class TemporaryDataOnDisk;
using TemporaryDataOnDiskPtr = std::shared_ptr<TemporaryDataOnDisk>;

class TemporaryFileStream;
using TemporaryFileStreamHolder = std::unique_ptr<TemporaryFileStream>;


/// Holds set of temporary files on disk and account amound of written data.
/// If limit is set, throws exception if limit is exceeded.
/// Data can be nested, so parent account all data written by children.
/// New file stream is created with `createStream`.
/// Streams are owned by this object and will be deleted when it is deleted.
class TemporaryDataOnDisk : boost::noncopyable
{
public:
    struct Stat
    {
        std::atomic<size_t> compressed_size;
        std::atomic<size_t> uncompressed_size;
    };

    explicit TemporaryDataOnDisk(VolumePtr volume_, size_t limit_)
        : volume(volume_), limit(limit_)
    {}

    explicit TemporaryDataOnDisk(const TemporaryDataOnDiskPtr & parent_, size_t limit_)
        : parent(parent_), volume(parent->volume), limit(limit_)
    {}

    TemporaryFileStream & createStream(CurrentMetrics::Value metric_scope, size_t reserve_size = 0);
    std::vector<TemporaryFileStreamHolder> & getStreams() { return streams; }

    const Stat & getStat() const { return stat; }

    /// TODO: remove
    /// Refactor all code that uses volume directly to use TemporaryDataOnDisk.
    VolumePtr getVolume() const { return volume; }

private:
    void deltaAlloc(int compressed_size, int uncompressed_size);

protected:
    void setAlloc(size_t compressed_size, size_t uncompressed_size);

    TemporaryDataOnDiskPtr parent = nullptr;
    VolumePtr volume;

    std::mutex mutex; /// Protects streams
    std::vector<TemporaryFileStreamHolder> streams;

    Stat stat;
    size_t limit = 0;
};

/// Data can be written into this stream and then read.
/// After finish writing, call `finishWriting` and then `read` to read the data.
/// It's a leaf node in temporary data tree described above.
class TemporaryFileStream final : private TemporaryDataOnDisk
{
public:
    using Stat = TemporaryDataOnDisk::Stat;
    using TemporaryDataOnDisk::getStat;

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, const TemporaryDataOnDiskPtr & parent_);

    void write(const Block & block);
    Stat finishWriting();
    bool isWriteFinished() const;

    Block read();

    ~TemporaryFileStream();

private:
    TemporaryFileOnDiskHolder file;

    struct OutputWriter;
    std::unique_ptr<OutputWriter> out_writer;

    struct InputReader;
    std::unique_ptr<InputReader> in_reader;
};

}
