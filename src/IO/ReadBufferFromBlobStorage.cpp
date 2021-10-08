#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <iostream>

#include <IO/ReadBufferFromBlobStorage.h>
#include <IO/ReadBufferFromString.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ReadBufferFromBlobStorage::ReadBufferFromBlobStorage(
    Azure::Storage::Blobs::BlobContainerClient blob_container_client_,
    const String & path_,
    UInt64 max_single_read_retries_,
    size_t /* buf_size_ */) :
    SeekableReadBuffer(nullptr, 0),
    blob_container_client(blob_container_client_),
    max_single_read_retries(max_single_read_retries_),
    path(path_) {}


bool ReadBufferFromBlobStorage::nextImpl()
{
    bool next_result = false;

    if (impl)
    {
        /// `impl` has been initialized earlier and now we're at the end of the current portion of data.
        impl->position() = position();
        assert(!impl->hasPendingData());
    }
    else
    {
        /// `impl` is not initialized and we're about to read the first portion of data.
        impl = initialize();
        next_result = impl->hasPendingData();
    }

    auto sleep_time_with_backoff_milliseconds = std::chrono::milliseconds(100);
    for (size_t attempt = 0; (attempt < max_single_read_retries) && !next_result; ++attempt)
    {
        try
        {
            /// Try to read a next portion of data.
            next_result = impl->next();
            break;
        }
        catch (const Exception & e)
        {
            // TODO: can't get this to compile, getting "error: reference to overloaded function could not be resolved; did you mean to call it?"
            // LOG_DEBUG(log, "Caught exception while reading Blob Storage object. Object: {}, Offset: {}, Attempt: {}, Message: {}",
            //     path, getPosition(), attempt, e.message());

            std::cout << "Caught exception while reading Blob Storage object. Object: " << path << ", Offset: "
                << getPosition() << ", Attempt: " << attempt << ", Message: " << e.message() << "\n";

            /// Pause before next attempt.
            std::this_thread::sleep_for(sleep_time_with_backoff_milliseconds);
            sleep_time_with_backoff_milliseconds *= 2;

            /// Try to reinitialize `impl`.
            impl.reset();
            impl = initialize();
            next_result = impl->hasPendingData();
        }
    }

    if (!next_result)
        return false;

    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset()); /// use the buffer returned by `impl`

    offset += working_buffer.size();

    return true;
}


off_t ReadBufferFromBlobStorage::seek(off_t offset_, int whence)
{
    if (impl)
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    offset = offset_;

    return offset;
}


off_t ReadBufferFromBlobStorage::getPosition()
{
    return offset - available();
}


std::unique_ptr<ReadBuffer> ReadBufferFromBlobStorage::initialize()
{
    auto blob_client = blob_container_client.GetBlobClient(path);
    auto prop = blob_client.GetProperties();
    auto blob_size = prop.Value.BlobSize;

#ifdef VERBOSE_DEBUG_MODE
    std::cout << "path: " << path << "\n";
    std::cout << "blob_size: " << blob_size << "\n";
#endif

    tmp_buffer.resize(blob_size);

    blob_client.DownloadTo(tmp_buffer.data(), blob_size);

    return std::make_unique<ReadBufferFromString>(tmp_buffer);
}

}

#endif
