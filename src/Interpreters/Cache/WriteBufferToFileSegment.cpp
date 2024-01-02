#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache.h>
#include <IO/SwapHelper.h>
#include <IO/ReadBufferFromFile.h>

#include <base/scope_guard.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

WriteBufferToFileSegment::WriteBufferToFileSegment(FileSegment * file_segment_)
    : WriteBufferFromFileDecorator(std::make_unique<WriteBufferFromFile>(file_segment_->getPathInLocalCache()))
    , file_segment(file_segment_)
{
}

WriteBufferToFileSegment::WriteBufferToFileSegment(FileSegmentsHolderPtr segment_holder_)
    : WriteBufferFromFileDecorator(
        segment_holder_->size() == 1
        ? std::make_unique<WriteBufferFromFile>(segment_holder_->front().getPathInLocalCache())
        : throw Exception(ErrorCodes::LOGICAL_ERROR, "WriteBufferToFileSegment can be created only from single segment"))
    , file_segment(&segment_holder_->front())
    , segment_holder(std::move(segment_holder_))
{
}

/// If it throws an exception, the file segment will be incomplete, so you should not use it in the future.
void WriteBufferToFileSegment::nextImpl()
{
    auto downloader [[maybe_unused]] = file_segment->getOrSetDownloader();
    chassert(downloader == FileSegment::getCallerId());

    SCOPE_EXIT({
        file_segment->completePartAndResetDownloader();
    });

    size_t bytes_to_write = offset();

    FileCacheReserveStat reserve_stat;
    /// In case of an error, we don't need to finalize the file segment
    /// because it will be deleted soon and completed in the holder's destructor.
    bool ok = file_segment->reserve(bytes_to_write, &reserve_stat);

    if (!ok)
    {
        String reserve_stat_msg;
        for (const auto & [kind, stat] : reserve_stat.stat_by_kind)
            reserve_stat_msg += fmt::format("{} hold {}, can release {}; ",
                toString(kind), ReadableSize(stat.non_releasable_size), ReadableSize(stat.releasable_size));

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
        LOG_WARNING(&Poco::Logger::get("WriteBufferToFileSegment"), "Failed to write to the underlying buffer ({})", file_segment->getInfoForLog());
        throw;
    }

    file_segment->setDownloadedSize(bytes_to_write);
}

std::shared_ptr<ReadBuffer> WriteBufferToFileSegment::getReadBufferImpl()
{
    finalize();
    return std::make_shared<ReadBufferFromFile>(file_segment->getPathInLocalCache());
}

WriteBufferToFileSegment::~WriteBufferToFileSegment()
{
    /// To be sure that file exists before destructor of segment_holder is called
    WriteBufferFromFileDecorator::finalize();
}

}
