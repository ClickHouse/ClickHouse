#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <boost/noncopyable.hpp>

#include <Common/CurrentMetrics.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Disks/IVolume.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <Interpreters/Cache/FileSegment.h>

#include <IO/ReadBufferFromFile.h>

class FileCacheTest_TemporaryDataReadBufferSize_Test;

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesUnknown;
}

namespace DB
{

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

class TemporaryDataBuffer;
using TemporaryDataBufferPtr = std::unique_ptr<TemporaryDataBuffer>;

class TemporaryFileHolder;

class FileCache;

struct TemporaryDataOnDiskSettings
{
    /// Max size on disk, if 0 there will be no limit
    size_t max_size_on_disk = 0;

    /// Compression codec for temporary data, if empty no compression will be used. LZ4 by default
    String compression_codec = "LZ4";

    /// Read/Write internal buffer size
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    /// Metrics counter to increment when temporary file in current scope are created
    CurrentMetrics::Metric current_metric = CurrentMetrics::TemporaryFilesUnknown;
};

/// Creates temporary files located on specified resource (disk, fs_cache, etc.)
using TemporaryFileProvider = std::function<std::unique_ptr<TemporaryFileHolder>(const TemporaryDataOnDiskSettings &, size_t)>;
TemporaryFileProvider createTemporaryFileProvider(VolumePtr volume);
TemporaryFileProvider createTemporaryFileProvider(FileCache * file_cache);

#if ENABLE_DISTRIBUTED_CACHE
struct DistributedCacheTag
{};

TemporaryFileProvider createTemporaryFileProvider(DistributedCacheTag);
#endif

/*
 * Used to account amount of temporary data written to disk.
 * If limit is set, throws exception if limit is exceeded.
 * Data can be nested, so parent scope accounts all data written by children.
 * Scopes are: global -> per-user -> per-query -> per-purpose (sorting, aggregation, etc).
 */
class TemporaryDataOnDiskScope : boost::noncopyable, public std::enable_shared_from_this<TemporaryDataOnDiskScope>
{
public:
    struct StatAtomic
    {
        std::atomic<size_t> compressed_size;
        std::atomic<size_t> uncompressed_size;
    };

    /// Root scope
    template <typename... Args>
    explicit TemporaryDataOnDiskScope(TemporaryDataOnDiskSettings settings_, Args &&... storage_args)
        : file_provider(createTemporaryFileProvider(std::forward<Args>(storage_args)...))
        , settings(std::move(settings_))
    {}


    TemporaryDataOnDiskScope(TemporaryDataOnDiskScopePtr parent_, TemporaryDataOnDiskSettings settings_)
        : parent(std::move(parent_))
        , file_provider(parent->file_provider)
        , settings(std::move(settings_))
    {}

    TemporaryDataOnDiskScopePtr childScope(CurrentMetrics::Metric current_metric, UInt64 buffer_size_ = 0);

    const TemporaryDataOnDiskSettings & getSettings() const { return settings; }
protected:
    friend class TemporaryDataBuffer;

    void deltaAllocAndCheck(ssize_t compressed_delta, ssize_t uncompressed_delta);

    TemporaryDataOnDiskScopePtr parent = nullptr;

    TemporaryFileProvider file_provider;

    StatAtomic stat;
    const TemporaryDataOnDiskSettings settings;
};

/** Used to hold the wrapper and wrapped object together.
  * This class provides a convenient way to manage the lifetime of both the wrapper and the wrapped object.
  * The wrapper class (Impl) stores a reference to the wrapped object (Holder), and both objects are owned by this class.
  * The lifetime of the wrapper and the wrapped object should be the same.
  * This pattern is commonly used when the caller only needs to interact with the wrapper and doesn't need to be aware of the wrapped object.
  * Examples: CompressedWriteBuffer and WriteBuffer, and NativeReader and ReadBuffer.
  */
template <typename Impl, typename Holder>
class WrapperGuard
{
public:
    template <typename ... Args>
    explicit WrapperGuard(std::unique_ptr<Holder> holder_, Args && ... args)
        : holder(std::move(holder_))
        , impl(std::make_unique<Impl>(*holder, std::forward<Args>(args)...))
    {
        chassert(holder);
        chassert(impl);
    }

    Impl * operator->() { chassert(impl); chassert(holder); return impl.get(); }
    const Impl * operator->() const { chassert(impl); chassert(holder); return impl.get(); }
    Impl & operator*() { chassert(impl); chassert(holder); return *impl; }
    const Impl & operator*() const { chassert(impl); chassert(holder); return *impl; }
    operator bool() const { return impl != nullptr; } /// NOLINT

    Holder * getHolder() { return holder.get(); }
    const Holder * getHolder() const { return holder.get(); }
    std::unique_ptr<Holder> releaseHolder()
    {
        impl.reset();
        return std::move(holder);
    }

    void reset()
    {
        impl.reset();
        holder.reset();
    }

protected:
    std::unique_ptr<Holder> holder;
    std::unique_ptr<Impl> impl;
};

/// Owns temporary file and provides access to it.
/// On destruction, file is removed and all resources are freed.
/// Lifetime of read/write buffers should be less than lifetime of TemporaryFileHolder.
class TemporaryFileHolder
{
public:
    explicit TemporaryFileHolder(CurrentMetrics::Metric current_metric_ = CurrentMetrics::TemporaryFilesUnknown);

    virtual std::unique_ptr<WriteBuffer> write() = 0;
    virtual std::unique_ptr<SeekableReadBuffer> read(size_t buffer_size) const = 0;

    virtual void releaseWriteBuffer(std::unique_ptr<WriteBuffer> /*write_buffer*/)
    {}

    /// Get location for logging
    virtual String describeFilePath() const = 0;

    virtual ~TemporaryFileHolder() = default;

private:
    CurrentMetrics::Increment metric_increment;
};

/// Reads raw data from temporary file
class TemporaryDataReadBuffer : public ReadBuffer
{
public:
    explicit TemporaryDataReadBuffer(std::unique_ptr<ReadBuffer> in_);

private:
    friend class ::FileCacheTest_TemporaryDataReadBufferSize_Test;

    bool nextImpl() override;

    WrapperGuard<CompressedReadBuffer, ReadBuffer> compressed_buf;
};

/// Writes raw data to buffer provided by file_holder, and accounts amount of written data in parent scope.
class TemporaryDataBuffer : public WriteBuffer
{
public:
    struct Stat
    {
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
    };

    explicit TemporaryDataBuffer(std::shared_ptr<TemporaryDataOnDiskScope> parent_, size_t reserve_size = 0);
    ~TemporaryDataBuffer() override;

    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

    std::unique_ptr<ReadBuffer> read();
    std::unique_ptr<SeekableReadBuffer> readRaw();

    CompressedWriteBuffer & getCompressedWriteBuffer();

    Stat finishWriting();

    Stat getStat() const;

    String describeFilePath() const;

private:
    void updateAllocAndCheck();
    void freeAlloc();

    std::shared_ptr<TemporaryDataOnDiskScope> parent;
    std::unique_ptr<TemporaryFileHolder> file_holder;
    WrapperGuard<CompressedWriteBuffer, WriteBuffer> out_compressed_buf;
    std::once_flag write_finished;

    Stat stat;
};


/// High level interfaces for reading and writing temporary data by blocks.
using TemporaryBlockStreamReaderHolder = WrapperGuard<NativeReader, ReadBuffer>;

class TemporaryBlockStreamHolder : public WrapperGuard<NativeWriter, TemporaryDataBuffer>
{
public:
    TemporaryBlockStreamHolder(SharedHeader header_, std::shared_ptr<TemporaryDataOnDiskScope> parent_, size_t reserve_size = 0);

    TemporaryBlockStreamReaderHolder getReadStream() const;

    TemporaryDataBuffer::Stat finishWriting() const;
    const Block & getHeader() const { return header; }

private:
    Block header;
};

}
