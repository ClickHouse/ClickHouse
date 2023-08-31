#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Interpreters/Cache/FileSegment.h>
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

    /// In case of an error, we don't need to finalize the file segment
    /// because it will be deleted soon and completed in the holder's destructor.
    bool ok = file_segment->reserve(bytes_to_write);
    if (!ok)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Failed to reserve space for the file cache ({})", file_segment->getInfoForLog());

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
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
