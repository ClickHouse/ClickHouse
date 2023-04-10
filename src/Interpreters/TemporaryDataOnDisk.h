#pragma once

#include <boost/noncopyable.hpp>

#include <Interpreters/Context.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/IVolume.h>
#include <Common/CurrentMetrics.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache.h>


namespace CurrentMetrics
{
    extern const Metric TemporaryFilesUnknown;
}

namespace DB
{

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

class TemporaryDataOnDisk;
using TemporaryDataOnDiskPtr = std::unique_ptr<TemporaryDataOnDisk>;

class TemporaryFileStream;
using TemporaryFileStreamPtr = std::unique_ptr<TemporaryFileStream>;

/*
 * Used to account amount of temporary data written to disk.
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

    explicit TemporaryDataOnDiskScope(VolumePtr volume_, FileCache * file_cache_, size_t limit_)
        : volume(std::move(volume_)), file_cache(file_cache_), limit(limit_)
    {}

    explicit TemporaryDataOnDiskScope(TemporaryDataOnDiskScopePtr parent_, size_t limit_)
        : parent(std::move(parent_)), volume(parent->volume), file_cache(parent->file_cache), limit(limit_)
    {}

    /// TODO: remove
    /// Refactor all code that uses volume directly to use TemporaryDataOnDisk.
    VolumePtr getVolume() const { return volume; }

protected:
    void deltaAllocAndCheck(ssize_t compressed_delta, ssize_t uncompressed_delta);

    TemporaryDataOnDiskScopePtr parent = nullptr;

    VolumePtr volume = nullptr;
    FileCache * file_cache = nullptr;

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
    friend class TemporaryFileStream; /// to allow it to call `deltaAllocAndCheck` to account data

public:
    using TemporaryDataOnDiskScope::StatAtomic;

    explicit TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_);

    explicit TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_, CurrentMetrics::Metric metric_scope);

    /// If max_file_size > 0, then check that there's enough space on the disk and throw an exception in case of lack of free space
    TemporaryFileStream & createStream(const Block & header, size_t max_file_size = 0);

    std::vector<TemporaryFileStream *> getStreams() const;
    bool empty() const;

    const StatAtomic & getStat() const { return stat; }

private:
    TemporaryFileStream & createStreamToCacheFile(const Block & header, size_t max_file_size);
    TemporaryFileStream & createStreamToRegularFile(const Block & header, size_t max_file_size);

    mutable std::mutex mutex;
    std::vector<TemporaryFileStreamPtr> streams TSA_GUARDED_BY(mutex);

    typename CurrentMetrics::Metric current_metric_scope = CurrentMetrics::TemporaryFilesUnknown;
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
        size_t num_rows = 0;
    };

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header_, TemporaryDataOnDisk * parent_);
    TemporaryFileStream(FileSegmentsHolder && segments_, const Block & header_, TemporaryDataOnDisk * parent_);

    size_t write(const Block & block);
    void flush();

    Stat finishWriting();
    bool isWriteFinished() const;

    Block read();

    String getPath() const;

    Block getHeader() const { return header; }

    /// Read finished and file released
    bool isEof() const;

    ~TemporaryFileStream();

private:
    void updateAllocAndCheck();

    /// Release everything, close reader and writer, delete file
    void release();

    TemporaryDataOnDisk * parent;

    Block header;

    /// Data can be stored in file directly or in the cache
    TemporaryFileOnDiskHolder file;
    FileSegmentsHolder segment_holder;

    Stat stat;

    struct OutputWriter;
    std::unique_ptr<OutputWriter> out_writer;

    struct InputReader;
    std::unique_ptr<InputReader> in_reader;
};

}
