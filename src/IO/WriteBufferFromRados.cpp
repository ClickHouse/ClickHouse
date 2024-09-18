#include <config.h>

#if USE_CEPH

#include "WriteBufferFromRados.h"

#include <utility>
#include <IO/WriteHelpers.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/ProfileEvents.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/Throttler.h>
#include <Common/logger_useful.h>
#include <Disks/ObjectStorages/Ceph/RadosObjectStorage.h>


namespace ProfileEvents
{
extern const Event WriteBufferFromRadosBytes;
extern const Event WriteBufferFromRadosMicroseconds;
extern const Event RemoteWriteThrottlerBytes;
extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

WriteBufferFromRados::WriteBufferFromRados(
    std::shared_ptr<RadosIOContext> io_ctx_,
    const String & object_id_,
    size_t buf_size_,
    const OSDSettings & osd_settings_,
    const WriteSettings & write_settings_,
    std::optional<std::map<String, String>> object_metadata_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , io_ctx(std::move(io_ctx_))
    , object_id(object_id_)
    , osd_settings(osd_settings_)
    , write_settings(write_settings_)
    , object_metadata(std::move(object_metadata_)) {}

size_t WriteBufferFromRados::writeImpl(const char * begin, size_t len)
{
    ResourceGuard rlock(ResourceGuard::Metrics::getIOWrite(), write_settings.io_scheduling.write_resource_link, len);
    size_t bytes_written = 0;
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WriteBufferFromRadosMicroseconds);

    if (first_write)
    {
        bytes_written = io_ctx->writeFull(current_chunk_name, begin, len);
        first_write = false;
    }
    else
        bytes_written = io_ctx->append(current_chunk_name, begin, len);

    rlock.unlock();
    if (write_settings.remote_throttler)
        write_settings.remote_throttler->add(len, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
    ProfileEvents::increment(ProfileEvents::WriteBufferFromRadosBytes, bytes_written);

    return bytes_written;
}

void WriteBufferFromRados::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;
    size_t byte_to_write = offset();

    while (bytes_written < byte_to_write)
    {
        size_t write_len = std::min(byte_to_write - bytes_written, osd_settings.osd_max_write_size);
        if (current_chunk_size_bytes + write_len > osd_settings.osd_max_object_size)
            startNewChunk();
        bytes_written += writeImpl(working_buffer.begin() + bytes_written, write_len);
        total_size += write_len;
        current_chunk_size_bytes += write_len;
    }
}

void WriteBufferFromRados::finalizeImpl()
{
    WriteBuffer::finalizeImpl();
    chassert(offset() == 0);
    LOG_TEST(log, "WriteBufferFromRados write to object {} {} bytes with {} chunks.", getFileName(), total_size, chunk_count);
    /// There can be empty objects, for example, spare columns
    if (chunk_count == 0 && total_size == 0)
        io_ctx->create(object_id);

    /// If object has multiple chunks, we need to update the metadata in HEAD object
    if (chunk_count > 1)
    {
        std::map<String, String> metadata;
        metadata[RadosStriper::XATTR_OBJECT_CHUNK_COUNT] = std::to_string(chunk_count);
        metadata[RadosStriper::XATTR_OBJECT_SIZE] = std::to_string(total_size);
        metadata[RadosStriper::XATTR_OBJECT_MTIME] = std::to_string(time(nullptr));
        if (object_metadata)
            metadata.insert(object_metadata->begin(), object_metadata->end());
        io_ctx->setAttributes(object_id, metadata);
    }
    else if (object_metadata)
    {
        io_ctx->setAttributes(object_id, *object_metadata);
    }

    all_chunks_finished = true;
}


WriteBufferFromRados::~WriteBufferFromRados()
{
    if (canceled)
    {
        LOG_INFO(
            log,
            "WriteBufferFromRados was canceled."
            "The file might not be written to Rados. "
            "{}.",
            object_id);
        return;
    }

    /// That destructor could be call with finalized=false in case of exceptions
    if (!finalized && !canceled)
    {
        LOG_INFO(
            log,
            "WriteBufferFromRados is not finalized in destructor. "
            "The file might not be written to Rados. "
            "{}.",
            object_id);
    }

    if (!all_chunks_finished)
    {
        LOG_WARNING(log, "Multipart upload hasn't finished, will abort all chunks. {}", object_id);
        tryAbortWrittenChunks();
    }
}

void WriteBufferFromRados::startNewChunk()
{
    current_chunk_name = chunk_count == 0 ? object_id : RadosStriper::getChunkName(object_id, chunk_count);
    current_chunk_size_bytes = 0;
    first_write = true;
    chunk_count++;
}

void WriteBufferFromRados::tryAbortWrittenChunks()
{
    try
    {
        auto remove_batch_size = osd_settings.objecter_inflight_ops;
        Strings chunk_names;
        for (size_t i = 1; i < chunk_count; ++i)
        {
            chunk_names.push_back(RadosStriper::getChunkName(object_id, i));
            if (chunk_names.size() >= remove_batch_size)
            {
                io_ctx->remove(chunk_names);
                chunk_names.clear();
            }
        }
        if (!chunk_names.empty())
            io_ctx->remove(chunk_names);
        /// The HEAD chunk
        io_ctx->remove(object_id, true);
    }
    catch (...)
    {
        LOG_ERROR(log, "Written chunks hasn't aborted. {}", getFileName());
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}

#endif
