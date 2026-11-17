#pragma once

#include <atomic>
#include <bitset>
#include <memory>
#include <mutex>
#include <boost/noncopyable.hpp>
#include <base/EnumReflection.h>

#include <Common/CurrentMetrics.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Disks/IVolume.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <Interpreters/FileCache/FileSegment.h>

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

struct TemporaryDataMetrics
{
    CurrentMetrics::Metric current_metric = CurrentMetrics::TemporaryFilesUnknown;
    std::optional<ProfileEvents::Event> bytes_compressed = {};
    std::optional<ProfileEvents::Event> bytes_uncompressed = {};
    std::optional<ProfileEvents::Event> num_files = {};

    /// Name of the operator that owns this temporary data
    std::string_view spilled_to_disk_operator = {};
};

struct TemporaryDataOnDiskSettings
{
    /// Max size on disk, if 0 there will be no limit
    size_t max_size_on_disk = 0;

    /// Compression codec for temporary data, if empty no compression will be used. LZ4 by default
    String compression_codec = {};

    /// Read/Write internal buffer size
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    /// Counters to update when files are created or data is written
    TemporaryDataMetrics metrics;
};

/// Creates temporary files located on specified resource (disk, fs_cache, etc.)
using TemporaryFileProvider = std::function<std::unique_ptr<TemporaryFileHolder>(const TemporaryDataOnDiskSettings &, size_t)>;
/// base_directory, if given, is where the files are placed on the volume's disk instead of its root; it must exist.
TemporaryFileProvider createTemporaryFileProvider(VolumePtr volume, const String & base_directory = "");
TemporaryFileProvider createTemporaryFileProvider(FileCache * file_cache);

/// Where the files of a scope are stored.
enum class TemporaryDataBackend : uint8_t
{
    Disk,
    LocalCache,
    DistributedCache,
};

inline TemporaryDataBackend getTemporaryDataBackend(const VolumePtr &, const String & = {}) { return TemporaryDataBackend::Disk; }
inline TemporaryDataBackend getTemporaryDataBackend(FileCache *) { return TemporaryDataBackend::LocalCache; }

#if ENABLE_DISTRIBUTED_CACHE
struct DistributedCacheTag
{};

TemporaryFileProvider createTemporaryFileProvider(DistributedCacheTag);
inline TemporaryDataBackend getTemporaryDataBackend(DistributedCacheTag) { return TemporaryDataBackend::DistributedCache; }
#endif

/// The outcome of reserving space for a temporary file before writing to it.
enum class TemporaryDataReserveResult : uint8_t
{
    Reserved,
    /// A limit of a scope or the space of the backend does not admit the bytes.
    NotEnoughSpace,
    /// The backend refused for a reason unrelated to its space, such as lock contention, and may admit the bytes later.
    Transient,
};

/// Where a writer places temporary data: the server's temporary storage, or the disk of the part being written.
enum class TemporaryDataPlacement : uint8_t
{
    TemporaryStorage,
    PartDisk,
};

/// A set of backends, one bit per value of `TemporaryDataBackend`.
using TemporaryDataBackends = std::bitset<magic_enum::enum_count<TemporaryDataBackend>()>;

inline TemporaryDataBackends makeTemporaryDataBackends(std::initializer_list<TemporaryDataBackend> backends)
{
    TemporaryDataBackends result;
    for (auto backend : backends)
        result.set(static_cast<size_t>(backend));
    return result;
}

/// A request for a scope of temporary data of a write.
struct TemporaryDataScopeRequest
{
    TemporaryDataPlacement placement = TemporaryDataPlacement::TemporaryStorage;
    /// The limit of the scope's compressed data; 0 for none of its own.
    size_t max_size = 0;
    /// The backends of the temporary storage the caller can use; not used for the part's disk.
    TemporaryDataBackends allowed_backends;
    /// Requests with the same placement and tag share one scope while any of them holds it.
    String tag;
    /// Count the scope's compressed and uncompressed bytes as they are written.
    std::optional<ProfileEvents::Event> bytes_compressed = {};
    std::optional<ProfileEvents::Event> bytes_uncompressed = {};
};

/// A limit of temporary data given as `max_size` bytes, or when they are 0 as `max_size_ratio` of `total_size`; 0 for
/// none, also when the ratio is given and the total size is unknown.
size_t getTemporaryDataLimit(size_t max_size, double max_size_ratio, std::optional<UInt64> total_size);

/*
 * Used to account amount of temporary data written to disk.
 * If limit is set, throws exception if limit is exceeded.
 * Data can be nested, so parent scope accounts all data written by children.
 * Scopes are: global -> per-user -> per-query -> per-purpose (sorting, aggregation, etc).
 */
class TemporaryDataOnDiskScope : boost::noncopyable, public std::enable_shared_from_this<TemporaryDataOnDiskScope>
{
public:
    /// Name of the gauge profile event reporting the current size of temporary data on disk for a query.
    static constexpr auto USAGE_EVENT_NAME = "TemporaryDataOnDiskUsage";

    struct StatAtomic
    {
        std::atomic<size_t> compressed_size;
        std::atomic<size_t> uncompressed_size;
    };

    /// Root scope
    template <typename... Args>
    explicit TemporaryDataOnDiskScope(TemporaryDataOnDiskSettings settings_, Args &&... storage_args)
        : backend(getTemporaryDataBackend(storage_args...))
        , file_provider(createTemporaryFileProvider(std::forward<Args>(storage_args)...))
        , settings(std::move(settings_))
        , codec(createCodec(settings))
    {}


    TemporaryDataOnDiskScope(TemporaryDataOnDiskScopePtr parent_, TemporaryDataOnDiskSettings settings_)
        : parent(std::move(parent_))
        , backend(parent->backend)
        , file_provider(parent->file_provider)
        , settings(std::move(settings_))
        , codec(createCodec(settings))
    {}

    TemporaryDataOnDiskScopePtr childScope(TemporaryDataMetrics metrics_, UInt64 buffer_size_ = 0, String compression_codec_ = {});

    const TemporaryDataOnDiskSettings & getSettings() const { return settings; }

    TemporaryDataBackend getBackend() const { return backend; }

    /// Currently used amount of temporary data on disk in bytes (compressed) for this scope.
    size_t currentCompressedSize() const { return stat.compressed_size.load(std::memory_order_relaxed); }

    /// Compressed bytes the limits of this scope and its ancestors still admit, the smallest of them; none without a
    /// limit in any of them.
    std::optional<size_t> getRemainingSize() const;

    /// The most compressed bytes that `bytes` written to a fresh block of a file of this scope can take: per block of
    /// the buffer size, the checksum and the codec's bound.
    size_t getCompressedBound(size_t bytes) const;

    /// Cumulative compressed bytes written by this scope and its children, including deleted files.
    size_t getSpilledBytes() const { return spilled_bytes.load(std::memory_order_relaxed); }
protected:
    friend class TemporaryDataBuffer;

    /// Charges this scope and its ancestors with a change of the data on disk. A positive `compressed_delta` that
    /// crosses the limit of some scope throws `TOO_MANY_ROWS_OR_BYTES` and leaves every scope unchanged.
    /// `written_compressed` is the compressed bytes that reached the file, which may be more than
    /// `compressed_delta` when they were reserved before.
    void deltaAllocAndCheck(ssize_t compressed_delta, ssize_t uncompressed_delta, size_t written_compressed);

    /// Adds `bytes` to the compressed size of this scope and its ancestors if none of their limits is crossed.
    /// Returns the scope whose limit refused, or null when every scope took the bytes; a refusal changes nothing.
    const TemporaryDataOnDiskScope * tryAddCompressedSize(size_t bytes);
    void subtractCompressedSize(size_t bytes);

    TemporaryDataOnDiskScopePtr parent = nullptr;

    const TemporaryDataBackend backend;
    TemporaryFileProvider file_provider;

    StatAtomic stat;
    std::atomic<size_t> spilled_bytes{0};
    const TemporaryDataOnDiskSettings settings;
    /// The codec of the settings, for the files of this scope and their bounds.
    const CompressionCodecPtr codec;

private:
    static CompressionCodecPtr createCodec(const TemporaryDataOnDiskSettings & scope_settings);
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
    [[nodiscard]] const Holder * getHolder() const { return holder.get(); }
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
    explicit TemporaryFileHolder(const TemporaryDataMetrics &);

    virtual std::unique_ptr<WriteBuffer> write() = 0;
    [[nodiscard]] virtual std::unique_ptr<SeekableReadBuffer> read(size_t buffer_size) const = 0;

    virtual void releaseWriteBuffer(std::unique_ptr<WriteBuffer> /*write_buffer*/)
    {}

    /// Adds `bytes` to the space reserved for the file in the backend, for backends that can reserve. Every byte written
    /// so far is in the file: a refusal may make the file unwritable, but leaves it readable.
    virtual TemporaryDataReserveResult tryReserveMore(size_t /*bytes*/) { return TemporaryDataReserveResult::Reserved; }
    /// Returns what the writes have not used of the reservations, for backends that can.
    virtual void releaseUnusedReservation() {}

    /// Get location for logging
    [[nodiscard]] virtual String describeFilePath() const = 0;

    /// The disk of the file, or null for a file in a cache. Returned by reference, because a copy for
    /// every append would contend on the reference count, which all the files on the disk share.
    [[nodiscard]] virtual const DiskPtr & getDisk() const;

    virtual ~TemporaryFileHolder() = default;

private:
    CurrentMetrics::Increment metric_increment;
};

/// Reads raw data from temporary file
class TemporaryDataReadBuffer : public ReadBuffer
{
public:
    explicit TemporaryDataReadBuffer(std::unique_ptr<ReadBuffer> in_);
    /// A reader that can be positioned with `seekToBlock`.
    explicit TemporaryDataReadBuffer(std::unique_ptr<SeekableReadBuffer> in_);

    /// Positions the reader at `position` returned by `TemporaryDataBuffer::startNewBlock`.
    void seekToBlock(size_t position);

private:
    friend class ::FileCacheTest_TemporaryDataReadBufferSize_Test;

    bool nextImpl() override;

    /// The file reader inside `compressed_buf`, when it is seekable.
    SeekableReadBuffer * seekable_in = nullptr;
    WrapperGuard<CompressedReadBuffer, ReadBuffer> compressed_buf;
};

struct ReservedTemporaryDataBuffer;

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

    /// Creates a buffer with `reserve_bytes` of compressed data reserved in the scopes and in the backend, so that the
    /// writes they cover hit no limit and no lack of space in either; a volume of several disks puts the file on one
    /// with room for them. A refusal creates nothing and reserves nothing.
    static ReservedTemporaryDataBuffer tryCreateWithReservation(std::shared_ptr<TemporaryDataOnDiskScope> parent_, size_t reserve_bytes);

    /// Writes every byte written so far to the file, past the buffers of the compression and of the file.
    void flushToFile();

    /// Adds `num_bytes` of compressed data to the reservation in the scopes and in the backend; called after
    /// flushToFile. After a refusal of the backend the file may take no more writes (the local cache marks its segment
    /// failed), but what it holds is finished and read as usual. A refusal reserves nothing.
    TemporaryDataReserveResult tryReserveMore(size_t num_bytes);

    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

    std::unique_ptr<ReadBuffer> read();
    std::unique_ptr<SeekableReadBuffer> readRaw(size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE);
    /// A reader positioned with `seekToBlock` at positions returned by `startNewBlock`; finishes writing if needed.
    std::unique_ptr<TemporaryDataReadBuffer> readSeekable();

    /// Puts the data written so far into separate compressed blocks and returns the position
    /// in the file from which the data written after this call can be read with `readFromPosition`.
    size_t startNewBlock();
    /// Reads the data from `position` returned by `startNewBlock`, finishes writing if needed.
    std::unique_ptr<ReadBuffer> readFromPosition(size_t position);

    CompressedWriteBuffer & getCompressedWriteBuffer();

    Stat finishWriting();

    Stat getStat() const;

    String describeFilePath() const;

    /// See `TemporaryFileHolder::getDisk`.
    const DiskPtr & getDisk() const;

private:
    TemporaryDataBuffer(std::shared_ptr<TemporaryDataOnDiskScope> parent_, std::unique_ptr<TemporaryFileHolder> file_holder_, size_t reserved_);

    void updateAllocAndCheck();
    void releaseReservation();
    void freeAlloc();

    std::shared_ptr<TemporaryDataOnDiskScope> parent;
    std::unique_ptr<TemporaryFileHolder> file_holder;
    WrapperGuard<CompressedWriteBuffer, WriteBuffer> out_compressed_buf;
    std::once_flag write_finished;

    Stat stat;
    /// Compressed bytes reserved in the scopes and not yet used by the writes.
    size_t reserved = 0;
    TemporaryDataMetrics metrics;

    bool reported_spilled_to_disk = false;
};

/// A buffer created with a reservation, or the reason the reservation was refused.
struct ReservedTemporaryDataBuffer
{
    TemporaryDataBufferPtr buffer;
    TemporaryDataReserveResult result = TemporaryDataReserveResult::Reserved;
};

/// Checks free space for the appends to existing temporary files, as the creation of a file checks its
/// reserve size. A writer that keeps appending to its files uses one object for each batch of appends,
/// and passes with every append the room that the rest of the batch needs: each disk is checked only at
/// the first append of the batch to a file on it, because a check costs a `statfs`. A file in a cache is
/// not checked, because the cache makes room for every write.
class TemporaryAppendSpaceCheck
{
public:
    /// Throws `NOT_ENOUGH_SPACE` unless the disk of `buffer` has room for `reserve_size` bytes, if the
    /// batch has not checked that disk yet. The space is reserved only for the duration of the check, as
    /// at the creation of a file.
    void beforeAppend(const TemporaryDataBuffer & buffer, size_t reserve_size);

private:
    /// The few disks of a temporary volume.
    std::vector<DiskPtr> checked_disks;
};


/// High level interfaces for reading and writing temporary data by blocks.
class TemporaryBlockStreamReaderHolder : public WrapperGuard<NativeReader, ReadBuffer>
{
public:
    using WrapperGuard<NativeReader, ReadBuffer>::WrapperGuard;
};


class TemporaryBlockStreamHolder : public WrapperGuard<NativeWriter, TemporaryDataBuffer>
{
public:
    TemporaryBlockStreamHolder(SharedHeader header_, std::shared_ptr<TemporaryDataOnDiskScope> parent_, size_t reserve_size = 0);

    [[nodiscard]] TemporaryBlockStreamReaderHolder getReadStream() const;

    TemporaryDataBuffer::Stat finishWriting() const; /// NOLINT(modernize-use-nodiscard): often called only for its side effect
    [[nodiscard]] const Block & getHeader() const { return header; }

private:
    Block header;
};

}
