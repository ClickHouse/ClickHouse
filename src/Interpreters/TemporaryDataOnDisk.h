#pragma once

#include <boost/noncopyable.hpp>

#include <Interpreters/Context.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IVolume.h>

namespace DB
{

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

class TemporaryDataOnDisk;
using TemporaryDataOnDiskHolder = std::unique_ptr<TemporaryDataOnDisk>;

class TemporaryFileStream;
using TemporaryFileStreamHolder = std::unique_ptr<TemporaryFileStream>;


/*
 * Used to account amount of temporary data written to dicsk.
 * If limit is set, throws exception if limit is exceeded.
 * Data can be nested, so parent scope accounts all data written by children.
 * Scopes are: global -> per-user -> per-query -> per-purpose (sorting, aggregation, etc).
 */
class TemporaryDataOnDiskScope : boost::noncopyable
{
public:
    struct StatAtomic
    {
        std::atomic<size_t> compressed_size;
        std::atomic<size_t> uncompressed_size;
    };

    explicit TemporaryDataOnDiskScope(VolumePtr volume_, size_t limit_)
        : volume(std::move(volume_)), limit(limit_)
    {}

    explicit TemporaryDataOnDiskScope(TemporaryDataOnDiskScopePtr parent_, size_t limit_)
        : parent(std::move(parent_)), volume(parent->volume), limit(limit_)
    {}

    /// TODO: remove
    /// Refactor all code that uses volume directly to use TemporaryDataOnDisk.
    VolumePtr getVolume() const { return volume; }

protected:
    void deltaAlloc(int compressed_size, int uncompressed_size);

    TemporaryDataOnDiskScopePtr parent = nullptr;
    VolumePtr volume;

    StatAtomic stat;
    size_t limit = 0;
};

/*
 * Holds the set of temporary files.
 * New file stream is created with `createStream`.
 * Streams are owned by this object and will be deleted when it is deleted.
 * It's a leaf node in temorarty data scope tree.
 */
class TemporaryDataOnDisk : private TemporaryDataOnDiskScope
{
    friend class TemporaryFileStream; /// to allow it to call `deltaAlloc` to account data
public:
    using TemporaryDataOnDiskScope::StatAtomic;

    explicit TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_)
        : TemporaryDataOnDiskScope(std::move(parent_), 0)
    {}

    TemporaryFileStream & createStream(const Block & header, CurrentMetrics::Value metric_scope, size_t reserve_size = 0);

    std::vector<TemporaryFileStream *> getStreams();
    bool empty() const { return streams.empty();}

    const StatAtomic & getStat() const { return stat; }

private:
    std::mutex mutex; /// Protects streams
    std::vector<TemporaryFileStreamHolder> streams;
};

/*
 * Data can be written into this stream and then read.
 * After finish writing, call `finishWriting` and then `read` to read the data.
 * Account amount of data written to disk in parent scope.
 */
class TemporaryFileStream : boost::noncopyable
{
public:
    struct Stat
    {
        /// Statistics for file
        /// Non-atomic because we don't allow to `read` or `write` into single file from multiple threads
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
        size_t written_rows = 0;
    };

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header_, TemporaryDataOnDisk * parent_);

    void write(const Block & block);
    Stat finishWriting();
    bool isWriteFinished() const;

    Block read();

    const String & path() const { return file->getPath(); }
    Block getHeader() const { return header; }

    ~TemporaryFileStream();

private:
    void updateAlloc(size_t rows_added);

    TemporaryDataOnDisk * parent;

    Block header;

    TemporaryFileOnDiskHolder file;

    Stat stat;

    struct OutputWriter;
    std::unique_ptr<OutputWriter> out_writer;

    struct InputReader;
    std::unique_ptr<InputReader> in_reader;
};

}
