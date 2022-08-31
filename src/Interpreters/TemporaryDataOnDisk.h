#pragma once

#include <boost/noncopyable.hpp>

#include <Interpreters/Context.h>
#include <Disks/TemporaryFileOnDisk.h>


namespace DB
{


class TemporaryFileStream;
using TemporaryFileStreamPtr = std::unique_ptr<TemporaryFileStream>;


/// Holds set of temporary files on disk and account amound of written data.
/// If limit is set, throws exception if limit is exceeded.
/// Data can be nested, so parent account all data written by children.
/// New file stream is created with `createStream`.
/// Streams are owned by this object and will be deleted when it is deleted.
class TemporaryDataOnDisk : boost::noncopyable
{
public:
    explicit TemporaryDataOnDisk(VolumePtr volume_, size_t limit_)
        : volume(volume_), limit(limit_)
    {}

    explicit TemporaryDataOnDisk(TemporaryDataOnDisk * parent_, size_t limit_)
        : parent(parent_), volume(parent->volume), limit(limit_)
    {}

    TemporaryFileStream & createStream();

private:
    void changeAlloc(size_t compressed_bytes, size_t uncompressed_bytes, bool is_dealloc);

protected:

    void alloc(size_t compressed_bytes, size_t uncompressed_bytes) { changeAlloc(compressed_bytes, uncompressed_bytes, false); }
    void dealloc(size_t compressed_bytes, size_t uncompressed_bytes) { changeAlloc(compressed_bytes, uncompressed_bytes, true); }

    struct Stat
    {
        std::atomic<size_t> compressed_bytes;
        std::atomic<size_t> uncompressed_bytes;
    };

    TemporaryDataOnDisk * parent = nullptr;
    VolumePtr volume;

    std::vector<TemporaryFileStreamPtr> streams;

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

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, TemporaryDataOnDisk * parent_);

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
