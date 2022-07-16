#include "ReadBufferFromRemoteFSGather.h"

#include <IO/SeekableReadBuffer.h>
#include <Disks/IO/ReadBufferFromWebServer.h>

#if USE_AWS_S3
#include <IO/ReadBufferFromS3.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <IO/ReadBufferFromAzureBlobStorage.h>
#endif

#if USE_HDFS
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#endif

#include <Disks/IO/CachedReadBufferFromFile.h>
#include <Common/logger_useful.h>
#include <filesystem>
#include <iostream>
#include <Common/hex.h>
#include <Interpreters/FilesystemCacheLog.h>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::shared_ptr<ReadBufferFromFileBase>
ReadBufferFromRemoteFSGather::createImplementationBuffer(const String & path, size_t file_size)
{
    if (!current_file_path.empty() && !with_cache && enable_cache_log)
    {
        appendFilesystemCacheLog();
    }

    current_file_path = path;
    current_file_size = file_size;
    total_bytes_read_from_current_file = 0;

    if (with_cache)
    {
        auto remote_file_reader_creator = [path, file_size, this]()
        {
            return createImplementationBufferImpl(path, file_size);
        };

        auto cache_key = settings.remote_fs_cache->hash(path);
        return std::make_shared<CachedReadBufferFromFile>(
            path,
            cache_key,
            settings.remote_fs_cache,
            std::move(remote_file_reader_creator),
            settings,
            query_id,
            file_size,
            /* allow_seeks */false,
            /* use_external_buffer */true,
            read_until_position ? std::optional<size_t>(read_until_position) : std::nullopt);
    }
    else
    {
        return createImplementationBufferImpl(path, file_size);
    }
}

#if USE_AWS_S3
std::shared_ptr<ReadBufferFromFileBase>
ReadBufferFromS3Gather::createImplementationBufferImpl(const String & path, size_t /* file_size */)
{
    return std::make_unique<ReadBufferFromS3>(
        client_ptr,
        bucket,
        path,
        version_id,
        max_single_read_retries,
        settings,
        /* use_external_buffer */true,
        /* offset */0,
        read_until_position,
        /* restricted_seek */true);
}
#endif


#if USE_AZURE_BLOB_STORAGE
std::shared_ptr<ReadBufferFromFileBase>
ReadBufferFromAzureBlobStorageGather::createImplementationBufferImpl(const String & path, size_t /* file_size */)
{
    return std::make_unique<ReadBufferFromAzureBlobStorage>(
        blob_container_client,
        path,
        settings,
        max_single_read_retries,
        max_single_download_retries,
        /* use_external_buffer */true,
        read_until_position);
}
#endif


std::shared_ptr<ReadBufferFromFileBase>
ReadBufferFromWebServerGather::createImplementationBufferImpl(const String & path, size_t /* file_size */)
{
    return std::make_unique<ReadBufferFromWebServer>(
        fs::path(uri) / path,
        context,
        settings,
        /* use_external_buffer */true,
        read_until_position);
}


#if USE_HDFS
std::shared_ptr<ReadBufferFromFileBase>
ReadBufferFromHDFSGather::createImplementationBufferImpl(const String & path, size_t /* file_size */)
{
    size_t begin_of_path = path.find('/', path.find("//") + 2);
    auto hdfs_path = path.substr(begin_of_path);
    auto hdfs_uri = path.substr(0, begin_of_path);
    LOG_TEST(log, "HDFS uri: {}, path: {}", hdfs_path, hdfs_uri);

    return std::make_unique<ReadBufferFromHDFS>(hdfs_uri, hdfs_path, config, settings);
}
#endif


ReadBufferFromRemoteFSGather::ReadBufferFromRemoteFSGather(
    const StoredObjects & blobs_to_read_,
    const ReadSettings & settings_)
    : ReadBuffer(nullptr, 0)
    , blobs_to_read(blobs_to_read_)
    , settings(settings_)
    , query_id(CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr ? CurrentThread::getQueryId() : "")
    , log(&Poco::Logger::get("ReadBufferFromRemoteFSGather"))
    , enable_cache_log(!query_id.empty() && settings.enable_filesystem_cache_log)
{
    if (blobs_to_read.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to read zero number of objects");

    with_cache = settings.remote_fs_cache
        && settings.enable_filesystem_cache
        && (!IFileCache::isReadOnly() || settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache);
}


void ReadBufferFromRemoteFSGather::appendFilesystemCacheLog()
{
    FilesystemCacheLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .query_id = query_id,
        .source_file_path = current_file_path,
        .file_segment_range = { 0, current_file_size },
        .read_type = FilesystemCacheLogElement::ReadType::READ_FROM_FS_BYPASSING_CACHE,
        .file_segment_size = total_bytes_read_from_current_file,
        .cache_attempted = false,
    };

    if (auto cache_log = Context::getGlobalContextInstance()->getFilesystemCacheLog())
        cache_log->add(elem);
}


IAsynchronousReader::Result ReadBufferFromRemoteFSGather::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    /**
     * Set `data` to current working and internal buffers.
     * Internal buffer with size `size`. Working buffer with size 0.
     */
    set(data, size);

    file_offset_of_buffer_end = offset;
    bytes_to_ignore = ignore;

    assert(!bytes_to_ignore || initialized());

    auto result = nextImpl();

    if (result)
        return {working_buffer.size(), BufferBase::offset()};

    return {0, 0};
}


void ReadBufferFromRemoteFSGather::initialize()
{
    /// One clickhouse file can be split into multiple files in remote fs.
    auto current_buf_offset = file_offset_of_buffer_end;
    for (size_t i = 0; i < blobs_to_read.size(); ++i)
    {
        const auto & object = blobs_to_read[i];

        if (object.bytes_size > current_buf_offset)
        {
            /// Do not create a new buffer if we already have what we need.
            if (!current_buf || current_buf_idx != i)
            {
                current_buf_idx = i;
                current_buf = createImplementationBuffer(object.absolute_path, object.bytes_size);
            }

            current_buf->seek(current_buf_offset, SEEK_SET);
            return;
        }

        current_buf_offset -= object.bytes_size;
    }
    current_buf_idx = blobs_to_read.size();
    current_buf = nullptr;
}


bool ReadBufferFromRemoteFSGather::nextImpl()
{
    /// Find first available buffer that fits to given offset.
    if (!current_buf)
        initialize();

    /// If current buffer has remaining data - use it.
    if (current_buf)
    {
        if (readImpl())
            return true;
    }
    else
    {
        return false;
    }

    if (!moveToNextBuffer())
    {
        return false;
    }

    return readImpl();
}


bool ReadBufferFromRemoteFSGather::moveToNextBuffer()
{
    /// If there is no available buffers - nothing to read.
    if (current_buf_idx + 1 >= blobs_to_read.size())
        return false;

    ++current_buf_idx;

    const auto & object = blobs_to_read[current_buf_idx];
    current_buf = createImplementationBuffer(object.absolute_path, object.bytes_size);

    return true;
}


bool ReadBufferFromRemoteFSGather::readImpl()
{
    swap(*current_buf);

    bool result = false;

    /**
     * Lazy seek is performed here.
     * In asynchronous buffer when seeking to offset in range [pos, pos + min_bytes_for_seek]
     * we save how many bytes need to be ignored (new_offset - position() bytes).
     */
    if (bytes_to_ignore)
    {
        total_bytes_read_from_current_file += bytes_to_ignore;
        current_buf->ignore(bytes_to_ignore);
        result = current_buf->hasPendingData();
        file_offset_of_buffer_end += bytes_to_ignore;
        bytes_to_ignore = 0;
    }

    if (!result)
        result = current_buf->next();

    if (blobs_to_read.size() == 1)
    {
        file_offset_of_buffer_end = current_buf->getFileOffsetOfBufferEnd();
    }
    else
    {
        /// For log family engines there are multiple s3 files for the same clickhouse file
        file_offset_of_buffer_end += current_buf->available();
    }

    swap(*current_buf);

    /// Required for non-async reads.
    if (result)
    {
        assert(available());
        nextimpl_working_buffer_offset = offset();
        total_bytes_read_from_current_file += available();
    }

    return result;
}


size_t ReadBufferFromRemoteFSGather::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}


void ReadBufferFromRemoteFSGather::setReadUntilPosition(size_t position)
{
    if (position != read_until_position)
    {
        read_until_position = position;
        reset();
    }
}


void ReadBufferFromRemoteFSGather::reset()
{
    current_buf.reset();
}

String ReadBufferFromRemoteFSGather::getFileName() const
{
    return current_file_path;
}


size_t ReadBufferFromRemoteFSGather::getFileSize() const
{
    size_t size = 0;
    for (const auto & object : blobs_to_read)
        size += object.bytes_size;
    return size;
}

String ReadBufferFromRemoteFSGather::getInfoForLog()
{
    if (!current_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get info: buffer not initialized");

    return current_buf->getInfoForLog();
}

size_t ReadBufferFromRemoteFSGather::getImplementationBufferOffset() const
{
    if (!current_buf)
        return file_offset_of_buffer_end;

    return current_buf->getFileOffsetOfBufferEnd();
}

ReadBufferFromRemoteFSGather::~ReadBufferFromRemoteFSGather()
{
    if (!with_cache && enable_cache_log)
    {
        appendFilesystemCacheLog();
    }
}

}
