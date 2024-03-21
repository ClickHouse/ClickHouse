#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context.h>
#include <IO/SwapHelper.h>
#include <IO/ReadBufferFromFile.h>

#include <Common/scope_guard_safe.h>

#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

namespace
{
    size_t getCacheLockWaitTimeout()
    {
        auto query_context = CurrentThread::getQueryContext();
        if (query_context)
            return query_context->getSettingsRef().temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds;
        else
            return Context::getGlobalContextInstance()->getSettingsRef().temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds;
    }
}

WriteBufferToFileSegment::WriteBufferToFileSegment(FileSegment * file_segment_)
    : WriteBufferFromFileDecorator(std::make_unique<WriteBufferFromFile>(file_segment_->getPath()))
    , file_segment(file_segment_)
    , reserve_space_lock_wait_timeout_milliseconds(getCacheLockWaitTimeout())
{
}

WriteBufferToFileSegment::WriteBufferToFileSegment(FileSegmentsHolderPtr segment_holder_)
    : WriteBufferFromFileDecorator(
        segment_holder_->size() == 1
        ? std::make_unique<WriteBufferFromFile>(segment_holder_->front().getPath())
        : throw Exception(ErrorCodes::LOGICAL_ERROR, "WriteBufferToFileSegment can be created only from single segment"))
    , file_segment(&segment_holder_->front())
    , segment_holder(std::move(segment_holder_))
    , reserve_space_lock_wait_timeout_milliseconds(getCacheLockWaitTimeout())
{
}

FilesystemCacheSizeAllocator::FilesystemCacheSizeAllocator(FileSegment & file_segment_, size_t lock_wait_timeout_)
    : file_segment(file_segment_)
    , lock_wait_timeout(lock_wait_timeout_)
{
    auto downloader [[maybe_unused]] = file_segment.getOrSetDownloader();
    chassert(downloader == FileSegment::getCallerId());
}

bool FilesystemCacheSizeAllocator::reserve(size_t size_in_bytes, FileCacheReserveStat * reserve_stat)
{
    total_size_in_bytes += size_in_bytes;
    return file_segment.reserve(size_in_bytes, lock_wait_timeout, reserve_stat);
}

void FilesystemCacheSizeAllocator::commit()
{
    file_segment.setDownloadedSize(total_size_in_bytes);
}

FilesystemCacheSizeAllocator::~FilesystemCacheSizeAllocator()
{
    try
    {
        /// Can throw LOGICAL_ERROR if some invariant is broken
        file_segment.completePartAndResetDownloader();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

/// If it throws an exception, the file segment will be incomplete, so you should not use it in the future.
void WriteBufferToFileSegment::nextImpl()
{
    auto downloader [[maybe_unused]] = file_segment->getOrSetDownloader();
    if (downloader != FileSegment::getCallerId())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Failed to set a downloader (current downloader: {}, file segment info: {})",
                        downloader, file_segment->getInfoForLog());
    }

    SCOPE_EXIT({
        if (file_segment->isDownloader())
            file_segment->completePartAndResetDownloader();
        else
            chassert(false);
    });

    size_t bytes_to_write = offset();
    if (bytes_to_write == 0)
        return;

    FilesystemCacheSizeAllocator cache_allocator(*file_segment, reserve_space_lock_wait_timeout_milliseconds);

    FileCacheReserveStat reserve_stat;
    /// In case of an error, we don't need to finalize the file segment
    /// because it will be deleted soon and completed in the holder's destructor.
    bool ok = cache_allocator.reserve(bytes_to_write, &reserve_stat);
    if (!ok)
    {
        String reserve_stat_msg;
        for (const auto & [kind, stat] : reserve_stat.stat_by_kind)
            reserve_stat_msg += fmt::format("{} hold {}, can release {}; ",
                toString(kind), ReadableSize(stat.non_releasable_size), ReadableSize(stat.releasable_size));

        if (std::filesystem::exists(file_segment->getPath()))
            std::filesystem::remove(file_segment->getPath());

        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Failed to reserve {} bytes for {}: {}(segment info: {})",
            bytes_to_write,
            file_segment->getKind() == FileSegmentKind::Temporary ? "temporary file" : "the file in cache",
            reserve_stat_msg,
            file_segment->getInfoForLog()
        );
    }

    try
    {
        SwapHelper swap(*this, *impl);
        /// Write data to the underlying buffer.
        impl->next();
    }
    catch (...)
    {
        LOG_WARNING(getLogger("WriteBufferToFileSegment"), "Failed to write to the underlying buffer ({})", file_segment->getInfoForLog());
        throw;
    }

    cache_allocator.commit();
}

std::unique_ptr<ReadBuffer> WriteBufferToFileSegment::getReadBufferImpl()
{
    finalize();
    return std::make_unique<ReadBufferFromFile>(file_segment->getPath());
}

WriteBufferToFileSegment::~WriteBufferToFileSegment()
{
    /// To be sure that file exists before destructor of segment_holder is called
    WriteBufferFromFileDecorator::finalize();
}

}
