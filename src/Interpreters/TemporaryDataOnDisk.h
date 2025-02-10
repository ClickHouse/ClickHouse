#pragma once

#include <atomic>
#include <mutex>
#include <boost/noncopyable.hpp>

#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Core/Block.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Common/CurrentMetrics.h>


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

class FileCache;

struct TemporaryDataOnDiskSettings
{
    /// Max size on disk, if 0 there will be no limit
    size_t max_size_on_disk = 0;

    /// Compression codec for temporary data, if empty no compression will be used. LZ4 by default
    String compression_codec = "LZ4";
};

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

    explicit TemporaryDataOnDiskScope(VolumePtr volume_, TemporaryDataOnDiskSettings settings_)
        : volume(std::move(volume_))
        , settings(std::move(settings_))
    {}

    explicit TemporaryDataOnDiskScope(VolumePtr volume_, FileCache * file_cache_, TemporaryDataOnDiskSettings settings_)
        : volume(std::move(volume_))
        , file_cache(file_cache_)
        , settings(std::move(settings_))
    {}

    explicit TemporaryDataOnDiskScope(TemporaryDataOnDiskScopePtr parent_, TemporaryDataOnDiskSettings settings_)
        : parent(std::move(parent_))
        , volume(parent->volume)
        , file_cache(parent->file_cache)
        , settings(std::move(settings_))
    {}

    /// TODO: remove
    /// Refactor all code that uses volume directly to use TemporaryDataOnDisk.
    VolumePtr getVolume() const { return volume; }

    const TemporaryDataOnDiskSettings & getSettings() const { return settings; }

protected:
    void deltaAllocAndCheck(ssize_t compressed_delta, ssize_t uncompressed_delta);

    TemporaryDataOnDiskScopePtr parent = nullptr;

    VolumePtr volume = nullptr;
    FileCache * file_cache = nullptr;

    StatAtomic stat;
    const TemporaryDataOnDiskSettings settings;
};

/*
 * Holds the set of temporary files.
 * New file stream is created with `createStream`.
 * Streams are owned by this object and will be deleted when it is deleted.
 * It's a leaf node in temporary data scope tree.
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

    /// Write raw data directly into buffer.
    /// Differences from `createStream`:
    ///   1) it doesn't account data in parent scope
    ///   2) returned buffer owns resources (instead of TemporaryDataOnDisk itself)
    /// If max_file_size > 0, then check that there's enough space on the disk and throw an exception in case of lack of free space
    std::unique_ptr<WriteBufferFromFileBase> createRawStream(size_t max_file_size = 0);

    std::vector<TemporaryFileStream *> getStreams() const;
    bool empty() const;

    const StatAtomic & getStat() const { return stat; }

private:
    FileSegmentsHolderPtr createCacheFile(size_t max_file_size);
    TemporaryFileOnDiskHolder createRegularFile(size_t max_file_size);

    mutable std::mutex mutex;
    std::vector<TemporaryFileStreamPtr> streams TSA_GUARDED_BY(mutex);

    typename CurrentMetrics::Metric current_metric_scope = CurrentMetrics::TemporaryFilesUnknown;
};

/*
 * Data can be written into this stream and then read.
 * After finish writing, call `finishWriting` and then either call `read` or 'getReadStream'(only one of the two) to read the data.
 * Account amount of data written to disk in parent scope.
 */
class TemporaryFileStream : boost::noncopyable
{
public:
    struct Reader
    {
        Reader(const String & path, const Block & header_, size_t size = 0);

        explicit Reader(const String & path, size_t size = 0);

        Block read();

        const std::string path;
        const size_t size;
        const std::optional<Block> header;

        std::unique_ptr<ReadBufferFromFileBase> in_file_buf;
        std::unique_ptr<CompressedReadBuffer> in_compressed_buf;
        std::unique_ptr<NativeReader> in_reader;
    };

    struct Stat
    {
        /// Statistics for file
        /// Non-atomic because we don't allow to `read` or `write` into single file from multiple threads
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
        size_t num_rows = 0;
    };

    TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header_, TemporaryDataOnDisk * parent_);
    TemporaryFileStream(FileSegmentsHolderPtr segments_, const Block & header_, TemporaryDataOnDisk * parent_);

    size_t write(const Block & block);
    void flush();

    Stat finishWriting();
    Stat finishWritingAsyncSafe();
    bool isWriteFinished() const;

    std::unique_ptr<Reader> getReadStream();

    Block read();

    String getPath() const;
    size_t getSize() const;

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
    FileSegmentsHolderPtr segment_holder;

    Stat stat;

    std::once_flag finish_writing;

    struct OutputWriter;
    std::unique_ptr<OutputWriter> out_writer;

    std::unique_ptr<Reader> in_reader;
};

}
