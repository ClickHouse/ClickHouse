#include <atomic>
#include <mutex>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/Cache/FileCache.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/WriteBufferFromTemporaryFile.h>

#include <Core/Defines.h>
#include <Common/formatReadable.h>
#include <Common/NaNUtils.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Common/Exception.h>

namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

inline CompressionCodecPtr getCodec(const TemporaryDataOnDiskSettings & settings)
{
    if (settings.compression_codec.empty())
        return CompressionCodecFactory::instance().get("NONE");

    return CompressionCodecFactory::instance().get(settings.compression_codec);
}

}

TemporaryFileHolder::TemporaryFileHolder()
{
    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);
}


class TemporaryFileInLocalCache : public TemporaryFileHolder
{
public:
    explicit TemporaryFileInLocalCache(FileCache & file_cache, size_t reserve_size = 0)
    {
        const auto key = FileSegment::Key::random();
        LOG_TRACE(getLogger("TemporaryFileInLocalCache"), "Creating temporary file in cache with key {}", key);
        segment_holder = file_cache.set(
            key, 0, std::max<size_t>(1, reserve_size),
            CreateFileSegmentSettings(FileSegmentKind::Ephemeral), FileCache::getCommonUser());

        chassert(segment_holder->size() == 1);
        segment_holder->front().getKeyMetadata()->createBaseDirectory(/* throw_if_failed */true);
    }

    std::unique_ptr<WriteBuffer> write() override
    {
        return std::make_unique<WriteBufferToFileSegment>(&segment_holder->front());
    }

    std::unique_ptr<ReadBuffer> read(size_t buffer_size) const override
    {
        return std::make_unique<ReadBufferFromFile>(segment_holder->front().getPath(), /* buf_size = */ buffer_size);
    }

    String describeFilePath() const override
    {
        return fmt::format("fscache://{}", segment_holder->front().getPath());
    }

private:
    FileSegmentsHolderPtr segment_holder;
};

class TemporaryFileOnLocalDisk : public TemporaryFileHolder
{
public:
    explicit TemporaryFileOnLocalDisk(VolumePtr volume, size_t reserve_size = 0)
        : path_to_file("tmp" + toString(UUIDHelpers::generateV4()))
    {
        LOG_TRACE(getLogger("TemporaryFileOnLocalDisk"), "Creating temporary file '{}'", path_to_file);
        if (reserve_size > 0)
        {
            auto reservation = volume->reserve(reserve_size);
            if (!reservation)
            {
                auto disks = volume->getDisks();
                Strings disks_info;
                for (const auto & d : disks)
                {
                    auto to_double = [](auto x) { return static_cast<double>(x); };
                    disks_info.push_back(fmt::format("{}: available: {} unreserved: {}, total: {}, keeping: {}",
                        d->getName(),
                        ReadableSize(d->getAvailableSpace().transform(to_double).value_or(NaNOrZero<double>())),
                        ReadableSize(d->getUnreservedSpace().transform(to_double).value_or(NaNOrZero<double>())),
                        ReadableSize(d->getTotalSpace().transform(to_double).value_or(NaNOrZero<double>())),
                        ReadableSize(d->getKeepingFreeSpace())));
                }

                throw Exception(ErrorCodes::NOT_ENOUGH_SPACE,
                    "Not enough space on temporary disk, cannot reserve {} bytes on [{}]",
                    reserve_size, fmt::join(disks_info, ", "));
            }
            disk = reservation->getDisk();
        }
        else
        {
            disk = volume->getDisk();
        }
        chassert(disk);
    }

    std::unique_ptr<WriteBuffer> write() override
    {
        return disk->writeFile(path_to_file);
    }

    std::unique_ptr<ReadBuffer> read(size_t buffer_size) const override
    {
        ReadSettings settings;
        settings.local_fs_buffer_size = buffer_size;
        settings.remote_fs_buffer_size = buffer_size;
        settings.prefetch_buffer_size = buffer_size;

        return disk->readFile(path_to_file, settings);
    }

    String describeFilePath() const override
    {
        return fmt::format("disk({})://{}/{}", disk->getName(), disk->getPath(), path_to_file);
    }

    ~TemporaryFileOnLocalDisk() override
    try
    {
        if (disk->existsFile(path_to_file))
        {
            LOG_TRACE(getLogger("TemporaryFileOnLocalDisk"), "Removing temporary file '{}'", path_to_file);
            disk->removeRecursive(path_to_file);
        }
        else
        {
            LOG_WARNING(getLogger("TemporaryFileOnLocalDisk"), "Temporary path '{}' does not exist in '{}' on disk {}", path_to_file, disk->getPath(), disk->getName());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

private:
    DiskPtr disk;
    String path_to_file;
};

TemporaryFileProvider createTemporaryFileProvider(VolumePtr volume)
{
    if (!volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Volume is not initialized");
    return [volume](size_t max_size) -> std::unique_ptr<TemporaryFileHolder>
    {
        return std::make_unique<TemporaryFileOnLocalDisk>(volume, max_size);
    };
}

TemporaryFileProvider createTemporaryFileProvider(FileCache * file_cache)
{
    if (!file_cache || !file_cache->isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File cache is not initialized");
    return [file_cache](size_t max_size) -> std::unique_ptr<TemporaryFileHolder>
    {
        return std::make_unique<TemporaryFileInLocalCache>(*file_cache, max_size);
    };
}

TemporaryDataOnDiskScopePtr TemporaryDataOnDiskScope::childScope(CurrentMetrics::Metric current_metric)
{
    TemporaryDataOnDiskSettings child_settings = settings;
    child_settings.current_metric = current_metric;
    return std::make_shared<TemporaryDataOnDiskScope>(shared_from_this(), child_settings);
}

TemporaryDataReadBuffer::TemporaryDataReadBuffer(std::unique_ptr<ReadBuffer> in_)
    : ReadBuffer(nullptr, 0)
    , compressed_buf(std::move(in_))
{
    BufferBase::set(compressed_buf->buffer().begin(), compressed_buf->buffer().size(), compressed_buf->offset());
}

bool TemporaryDataReadBuffer::nextImpl()
{
    compressed_buf->position() = position();
    if (!compressed_buf->next())
    {
        set(compressed_buf->position(), 0);
        return false;
    }
    BufferBase::set(compressed_buf->buffer().begin(), compressed_buf->buffer().size(), compressed_buf->offset());
    return true;
}

TemporaryDataBuffer::TemporaryDataBuffer(TemporaryDataOnDiskScope * parent_, size_t reserve_size)
    : WriteBuffer(nullptr, 0)
    , parent(parent_)
    , file_holder(parent->file_provider(reserve_size))
    , out_compressed_buf(file_holder->write(), getCodec(parent->getSettings()))
{
    WriteBuffer::set(out_compressed_buf->buffer().begin(), out_compressed_buf->buffer().size());
}

void TemporaryDataBuffer::nextImpl()
{
    if (!out_compressed_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file buffer writing has been finished");

    out_compressed_buf->position() = position();
    out_compressed_buf->next();
    BufferBase::set(out_compressed_buf->buffer().begin(), out_compressed_buf->buffer().size(), out_compressed_buf->offset());
    updateAllocAndCheck();
}

String TemporaryDataBuffer::describeFilePath() const
{
    return file_holder->describeFilePath();
}

void TemporaryDataBuffer::cancelImpl() noexcept
{
    if (out_compressed_buf)
    {
        /// CompressedWriteBuffer doesn't call cancel/finalize for wrapped buffer
        out_compressed_buf->cancel();
        out_compressed_buf.getHolder()->cancel();
    }
}

void TemporaryDataBuffer::finalizeImpl()
{
    if (!out_compressed_buf)
        return;

    /// CompressedWriteBuffer doesn't call cancel/finalize for wrapped buffer
    out_compressed_buf->finalize();
    out_compressed_buf.getHolder()->finalize();

    updateAllocAndCheck();
    out_compressed_buf.reset();
}

TemporaryDataBuffer::Stat TemporaryDataBuffer::finishWriting()
{
    /// TemporaryDataBuffer::read can be called from multiple threads
    std::call_once(write_finished, [this]
    {
        if (canceled)
            throw Exception(ErrorCodes::INVALID_STATE, "Writing to temporary file buffer was not successful");
        next();
        finalize();
    });
    return stat;
}

std::unique_ptr<ReadBuffer> TemporaryDataBuffer::read()
{
    finishWriting();

    if (stat.compressed_size == 0 && stat.uncompressed_size == 0)
        return std::make_unique<TemporaryDataReadBuffer>(std::make_unique<ReadBufferFromEmptyFile>());

    /// Keep buffer size less that file size, to avoid memory overhead for large amounts of small files
    size_t buffer_size = std::min<size_t>(stat.compressed_size, DBMS_DEFAULT_BUFFER_SIZE);
    return std::make_unique<TemporaryDataReadBuffer>(file_holder->read(buffer_size));
}

void TemporaryDataBuffer::updateAllocAndCheck()
{
    if (!out_compressed_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary file buffer writing has been finished");

    size_t new_compressed_size = out_compressed_buf->getCompressedBytes();
    size_t new_uncompressed_size = out_compressed_buf->getUncompressedBytes();

    if (unlikely(new_compressed_size < stat.compressed_size || new_uncompressed_size < stat.uncompressed_size))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Temporary file {} size decreased after write: compressed: {} -> {}, uncompressed: {} -> {}",
            file_holder ? file_holder->describeFilePath() : "NULL",
            new_compressed_size, stat.compressed_size, new_uncompressed_size, stat.uncompressed_size);
    }

    parent->deltaAllocAndCheck(new_compressed_size - stat.compressed_size, new_uncompressed_size - stat.uncompressed_size);
    stat.compressed_size = new_compressed_size;
    stat.uncompressed_size = new_uncompressed_size;
}

void TemporaryDataOnDiskScope::deltaAllocAndCheck(ssize_t compressed_delta, ssize_t uncompressed_delta)
{
    if (parent)
        parent->deltaAllocAndCheck(compressed_delta, uncompressed_delta);

    /// check that we don't go negative
    if ((compressed_delta < 0 && stat.compressed_size < static_cast<size_t>(-compressed_delta)) ||
        (uncompressed_delta < 0 && stat.uncompressed_size < static_cast<size_t>(-uncompressed_delta)))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Negative temporary data size");
    }

    size_t new_consumption = stat.compressed_size + compressed_delta;
    if (compressed_delta > 0 && settings.max_size_on_disk && new_consumption > settings.max_size_on_disk)
        throw Exception(ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "Limit for temporary files size exceeded (would consume {} / {} bytes)", new_consumption, settings.max_size_on_disk);

    stat.compressed_size += compressed_delta;
    stat.uncompressed_size += uncompressed_delta;
}

TemporaryBlockStreamHolder::TemporaryBlockStreamHolder(const Block & header_, TemporaryDataOnDiskScope * parent_, size_t reserve_size)
    : WrapperGuard(std::make_unique<TemporaryDataBuffer>(parent_, reserve_size), DBMS_TCP_PROTOCOL_VERSION, header_)
    , header(header_)
{}

TemporaryDataBuffer::Stat TemporaryBlockStreamHolder::finishWriting() const
{
    if (!holder)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary block stream is not initialized");

    impl->flush();
    return holder->finishWriting();
}

TemporaryBlockStreamReaderHolder TemporaryBlockStreamHolder::getReadStream() const
{
    if (!holder)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary block stream is not initialized");
    return TemporaryBlockStreamReaderHolder(holder->read(), header, DBMS_TCP_PROTOCOL_VERSION);
}

TemporaryDataBuffer::~TemporaryDataBuffer()
{
    if (!finalized)
        cancel();
}
}
