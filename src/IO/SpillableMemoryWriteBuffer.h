#pragma once

#include <base/types.h>
#include <IO/MemoryReadWriteBuffer.h>

#include <atomic>
#include <functional>
#include <memory>

namespace DB
{

class ReadBuffer;
class WriteBufferFromFileBase;

/// MemoryWriteBuffer whose in-memory usage is bounded by a shared SpillChecker:
/// once its capacity would be exceeded, buffered data is spilled to a target
/// created by `SpillConfig::write_creator` and the memory is released.
/// After finalize() the whole stream can be read back via tryGetReadBuffer(); the
/// spilled part comes first, followed by the in-memory part (reused from
/// MemoryWriteBuffer::getReadBufferImpl).
class SpillableMemoryWriteBuffer : public MemoryWriteBuffer
{
public:
    /// Tracks the memory used by the buffered data (shared between several buffers)
    /// and decides when it is spillable.
    class SpillChecker
    {
    public:
        explicit SpillChecker(size_t max_capacity_)
            : max_capacity(max_capacity_)
        {
        }

        Int64 get() const { return amount.load(std::memory_order_relaxed); }

        size_t getMaxCapacity() const { return max_capacity; }

        void alloc(Int64 size) { amount.fetch_add(size, std::memory_order_relaxed); }

        void dealloc(Int64 size) { amount.fetch_sub(size, std::memory_order_relaxed); }

        /// True if allocating `additional` more bytes would exceed the capacity.
        bool isSpillable(Int64 additional) const
        {
            return static_cast<Int64>(max_capacity) - get() < additional;
        }

    private:
        const size_t max_capacity;
        std::atomic<Int64> amount{0};
    };

    /// Creates the write buffer that spilled data is written to (created lazily on
    /// the first spill and reused for all subsequent ones). `filename` is the spill
    /// target path.
    using SpillFileWriterCreator = std::function<std::unique_ptr<WriteBufferFromFileBase>(const String & filename)>;

    /// Creates the read buffer over the spilled data, used by tryGetReadBuffer()
    /// to return the spilled part. `filename` is the spill target path.
    using SpillFileReadBufferCreator = std::function<std::unique_ptr<ReadBuffer>(const String & filename)>;

    /// Creates the temp directory for spill files; called lazily on the first
    /// spill. The write/read creators assemble the full path themselves.
    using SpillTempDirCreator = std::function<void()>;
    /// Removes the temp directory with spill files; called from ~PackedFilesWriter.
    using SpillTempDirRemover = std::function<void()>;

    /// Spill settings: `checker` bounds the in-memory usage; the spill files are
    /// created lazily in the temp directory returned by `create_spill_temp_dir`.
    struct SpillConfig
    {
        SpillChecker checker;
        SpillFileWriterCreator write_creator;
        SpillFileReadBufferCreator read_creator;
        SpillTempDirCreator create_spill_temp_dir;
        SpillTempDirRemover remove_spill_temp_dir;

        SpillConfig(
            size_t max_capacity_,
            SpillFileWriterCreator write_creator_,
            SpillFileReadBufferCreator read_creator_,
            SpillTempDirCreator create_spill_temp_dir_,
            SpillTempDirRemover remove_spill_temp_dir_)
            : checker(max_capacity_)
            , write_creator(std::move(write_creator_))
            , read_creator(std::move(read_creator_))
            , create_spill_temp_dir(std::move(create_spill_temp_dir_))
            , remove_spill_temp_dir(std::move(remove_spill_temp_dir_))
        {
        }
    };

    /// `spill_config_` == nullptr disables spilling (all data stays in memory).
    /// `spill_filename` is the file the spilled data is written to and read back from.
    SpillableMemoryWriteBuffer(
        std::shared_ptr<SpillConfig> spill_config_,
        String filename_,
        size_t initial_chunk_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        double growth_rate_ = 2.0,
        size_t max_chunk_size_ = 128 * DBMS_DEFAULT_BUFFER_SIZE);

    ~SpillableMemoryWriteBuffer() override;

    /// Name of the file the spilled data is written to (empty if spilling is disabled).
    const String & getFileName() const { return filename; }

    /// Force flushing of all currently buffered data to the spill target.
    void spill();

protected:
    void nextImpl() override;
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

    std::unique_ptr<ReadBuffer> getReadBufferImpl() override;

private:
    void addChunkAndTrack();
    void spillImpl();

    std::shared_ptr<SpillConfig> spill_config;
    String filename;
    bool spill_dir_created = false;
    std::unique_ptr<WriteBufferFromFileBase> spill_buffer;
};

}
