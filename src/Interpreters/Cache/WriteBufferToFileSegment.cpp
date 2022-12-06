#include <Interpreters/Cache/WriteBufferToFileSegment.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
}

namespace
{
    class SwapHelper
    {
    public:
        SwapHelper(WriteBuffer & b1_, WriteBuffer & b2_) : b1(b1_), b2(b2_) { b1.swap(b2); }
        ~SwapHelper() { b1.swap(b2); }

    private:
        WriteBuffer & b1;
        WriteBuffer & b2;
    };
}

WriteBufferToFileSegment::WriteBufferToFileSegment(std::unique_ptr<WriteBuffer> impl_, FileSegment * file_segment_)
    : WriteBufferFromFileDecorator(std::move(impl_)), file_segment(file_segment_)
{
    auto downloader = file_segment->getOrSetDownloader();
    if (downloader != FileSegment::getCallerId())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set a downloader. ({})", file_segment->getInfoForLog());
}

void WriteBufferToFileSegment::nextImpl()
{
    size_t bytes_to_write = offset();
    LOG_WARNING(&Poco::Logger::get("WriteBufferToFileSegment"), "Writing {} bytes", bytes_to_write);

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
        file_segment->completeWithState(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
        if (file_segment->isDownloader())
            file_segment->completePartAndResetDownloader();
        throw;
    }
    LOG_WARNING(&Poco::Logger::get("WriteBufferToFileSegment"), "Written {} bytes", bytes_to_write);

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


void WriteBufferToFileSegment::finalizeImpl()
{
    try
    {
        WriteBufferFromFileDecorator::finalizeImpl();
        /// Do not reset and remove temporary file_segment, because the it will be used for reading
    }
    catch (...)
    {
        file_segment->completeWithState(FileSegment::State::PARTIALLY_DOWNLOADED_NO_CONTINUATION);
        if (file_segment->isDownloader())
            file_segment->completePartAndResetDownloader();
        throw;
    }
}

}
