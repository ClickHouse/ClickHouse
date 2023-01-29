#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Interpreters/Cache/FileSegment.h>
#include <IO/SwapHelper.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
}

WriteBufferToFileSegment::WriteBufferToFileSegment(FileSegment * file_segment_)
    : WriteBufferFromFileDecorator(file_segment_->detachWriter()), file_segment(file_segment_)
{
    auto downloader = file_segment->getOrSetDownloader();
    if (downloader != FileSegment::getCallerId())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set a downloader. ({})", file_segment->getInfoForLog());
}

/// If it throws an exception, the file segment will be incomplete, so you should not use it in the future.
void WriteBufferToFileSegment::nextImpl()
{
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
