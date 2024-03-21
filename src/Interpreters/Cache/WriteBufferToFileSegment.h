#pragma once

#include <IO/WriteBufferFromFileDecorator.h>
#include <Interpreters/Cache/FileSegment.h>
#include <IO/IReadableWriteBuffer.h>

namespace DB
{

class FileSegment;

/// Writes data to filesystem cache
class WriteBufferToFileSegment : public WriteBufferFromFileDecorator, public IReadableWriteBuffer
{
public:
    explicit WriteBufferToFileSegment(FileSegment * file_segment_);
    explicit WriteBufferToFileSegment(FileSegmentsHolderPtr segment_holder);

    void nextImpl() override;
    ~WriteBufferToFileSegment() override;

private:

    std::unique_ptr<ReadBuffer> getReadBufferImpl() override;

    /// Reference to the file segment in segment_holder if owned by this WriteBufferToFileSegment
    /// or to the external file segment passed to the constructor
    FileSegment * file_segment;

    /// Empty if file_segment is not owned by this WriteBufferToFileSegment
    FileSegmentsHolderPtr segment_holder;

    const size_t reserve_space_lock_wait_timeout_milliseconds;
};

/// Allocates space in filesystem cache for file segment
/// Maintains invariants for file segment that should be kept during before and after `reserve` call
class FilesystemCacheSizeAllocator
{
public:
    explicit FilesystemCacheSizeAllocator(FileSegment & file_segment_, size_t lock_wait_timeout_ = 1000);

    /// Should be called before actual write to the file
    /// Returns true if space was successfully reserved
    bool reserve(size_t size_in_bytes, FileCacheReserveStat * reserve_stat = nullptr);

    /// Should be called after sucessful reserve and actual write to the file
    void commit();

    ~FilesystemCacheSizeAllocator();

private:
    FileSegment & file_segment;
    size_t lock_wait_timeout;
    size_t total_size_in_bytes = 0;
};

}
