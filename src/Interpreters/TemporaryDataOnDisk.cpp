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
#include <Disks/SingleDiskVolume.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/WriteBufferFromTemporaryFile.h>

#include <Core/Defines.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include "Common/Exception.h"

namespace ProfileEvents
{
    extern const Event ExternalProcessingFilesTotal;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
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

TemporaryDataOnDisk::TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_)
    : TemporaryDataOnDiskScope(parent_, parent_->getSettings())
{}

TemporaryDataOnDisk::TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_, CurrentMetrics::Metric metric_scope)
    : TemporaryDataOnDiskScope(parent_, parent_->getSettings())
    , current_metric_scope(metric_scope)
{}

std::unique_ptr<WriteBufferFromFileBase> TemporaryDataOnDisk::createRawStream(size_t max_file_size)
{
    if (file_cache && file_cache->isInitialized())
    {
        auto holder = createCacheFile(max_file_size);
        return std::make_unique<WriteBufferToFileSegment>(std::move(holder));
    }
    else if (volume)
    {
        auto tmp_file = createRegularFile(max_file_size);
        return std::make_unique<WriteBufferFromTemporaryFile>(std::move(tmp_file));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDiskScope has no cache and no volume");
}

TemporaryFileStream & TemporaryDataOnDisk::createStream(const Block & header, size_t max_file_size)
{
    if (file_cache && file_cache->isInitialized())
    {
        auto holder = createCacheFile(max_file_size);

        std::lock_guard lock(mutex);
        TemporaryFileStreamPtr & tmp_stream = streams.emplace_back(std::make_unique<TemporaryFileStream>(std::move(holder), header, this));
        return *tmp_stream;
    }
    else if (volume)
    {
        auto tmp_file = createRegularFile(max_file_size);
        std::lock_guard lock(mutex);
        TemporaryFileStreamPtr & tmp_stream = streams.emplace_back(std::make_unique<TemporaryFileStream>(std::move(tmp_file), header, this));
        return *tmp_stream;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDiskScope has no cache and no volume");
}

FileSegmentsHolderPtr TemporaryDataOnDisk::createCacheFile(size_t max_file_size)
{
    if (!file_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDiskScope has no cache");

    ProfileEvents::increment(ProfileEvents::ExternalProcessingFilesTotal);

    const auto key = FileSegment::Key::random();
    auto holder = file_cache->set(
        key, 0, std::max(10_MiB, max_file_size),
        CreateFileSegmentSettings(FileSegmentKind::Ephemeral), FileCache::getCommonUser());

    chassert(holder->size() == 1);
    holder->back().getKeyMetadata()->createBaseDirectory(/* throw_if_failed */true);

    return holder;
}

TemporaryFileOnDiskHolder TemporaryDataOnDisk::createRegularFile(size_t max_file_size)
{
    if (!volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDiskScope has no volume");

    DiskPtr disk;
    if (max_file_size > 0)
    {
        auto reservation = volume->reserve(max_file_size);
        if (!reservation)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space on temporary disk");
        disk = reservation->getDisk();
    }
    else
    {
        disk = volume->getDisk();
    }
    /// We do not increment ProfileEvents::ExternalProcessingFilesTotal here because it is incremented in TemporaryFileOnDisk constructor.
    return std::make_unique<TemporaryFileOnDisk>(disk, current_metric_scope);
}

std::vector<TemporaryFileStream *> TemporaryDataOnDisk::getStreams() const
{
    std::vector<TemporaryFileStream *> res;
    std::lock_guard lock(mutex);
    res.reserve(streams.size());
    for (const auto & stream : streams)
        res.push_back(stream.get());
    return res;
}

bool TemporaryDataOnDisk::empty() const
{
    std::lock_guard lock(mutex);
    return streams.empty();
}

static inline CompressionCodecPtr getCodec(const TemporaryDataOnDiskSettings & settings)
{
    if (settings.compression_codec.empty())
        return CompressionCodecFactory::instance().get("NONE");

    return CompressionCodecFactory::instance().get(settings.compression_codec);
}

struct TemporaryFileStream::OutputWriter
{
    OutputWriter(std::unique_ptr<WriteBuffer> out_buf_, const Block & header_, const TemporaryDataOnDiskSettings & settings)
        : out_buf(std::move(out_buf_))
        , out_compressed_buf(*out_buf, getCodec(settings))
        , out_writer(out_compressed_buf, DBMS_TCP_PROTOCOL_VERSION, header_)
    {
    }

    size_t write(const Block & block)
    {
        if (finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write to finalized stream");
        size_t written_bytes = out_writer.write(block);
        num_rows += block.rows();
        return written_bytes;
    }

    void flush()
    {
        if (finalized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot flush finalized stream");

        out_compressed_buf.next();
        out_buf->next();
        out_writer.flush();
    }

    void finalize()
    {
        if (finalized)
            return;

        /// if we called finalize() explicitly, and got an exception,
        /// we don't want to get it again in the destructor, so set finalized flag first
        finalized = true;

        out_writer.flush();
        out_compressed_buf.finalize();
        out_buf->finalize();
    }

    ~OutputWriter()
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

    std::unique_ptr<WriteBuffer> out_buf;
    CompressedWriteBuffer out_compressed_buf;
    NativeWriter out_writer;

    std::atomic_size_t num_rows = 0;

    bool finalized = false;
};

TemporaryFileStream::Reader::Reader(const String & path_, const Block & header_, size_t size_)
    : path(path_)
    , size(size_ ? std::min<size_t>(size_, DBMS_DEFAULT_BUFFER_SIZE) : DBMS_DEFAULT_BUFFER_SIZE)
    , header(header_)
{
    LOG_TEST(getLogger("TemporaryFileStream"), "Reading {} from {}", header_.dumpStructure(), path);
}

TemporaryFileStream::Reader::Reader(const String & path_, size_t size_)
    : path(path_)
    , size(size_ ? std::min<size_t>(size_, DBMS_DEFAULT_BUFFER_SIZE) : DBMS_DEFAULT_BUFFER_SIZE)
{
    LOG_TEST(getLogger("TemporaryFileStream"), "Reading from {}", path);
}

Block TemporaryFileStream::Reader::read()
{
    if (!in_reader)
    {
        if (fs::exists(path))
            in_file_buf = std::make_unique<ReadBufferFromFile>(path, size);
        else
            in_file_buf = std::make_unique<ReadBufferFromEmptyFile>();

        in_compressed_buf = std::make_unique<CompressedReadBuffer>(*in_file_buf);
        if (header.has_value())
            in_reader = std::make_unique<NativeReader>(*in_compressed_buf, header.value(), DBMS_TCP_PROTOCOL_VERSION);
        else
            in_reader = std::make_unique<NativeReader>(*in_compressed_buf, DBMS_TCP_PROTOCOL_VERSION);
    }
    return in_reader->read();
}

TemporaryFileStream::TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header_, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , header(header_)
    , file(std::move(file_))
    , out_writer(std::make_unique<OutputWriter>(std::make_unique<WriteBufferFromFile>(file->getAbsolutePath()), header, parent->settings))
{
    LOG_TEST(getLogger("TemporaryFileStream"), "Writing to temporary file {}", file->getAbsolutePath());
}

TemporaryFileStream::TemporaryFileStream(FileSegmentsHolderPtr segments_, const Block & header_, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , header(header_)
    , segment_holder(std::move(segments_))
{
    if (segment_holder->size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryFileStream can be created only from single segment");
    auto out_buf = std::make_unique<WriteBufferToFileSegment>(&segment_holder->front());

    LOG_TEST(getLogger("TemporaryFileStream"), "Writing to temporary file {}", out_buf->getFileName());
    out_writer = std::make_unique<OutputWriter>(std::move(out_buf), header, parent_->settings);
}

size_t TemporaryFileStream::write(const Block & block)
{
    if (!out_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing has been finished");

    updateAllocAndCheck();
    size_t bytes_written = out_writer->write(block);
    return bytes_written;
}

void TemporaryFileStream::flush()
{
    if (!out_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing has been finished");

    out_writer->flush();
}

TemporaryFileStream::Stat TemporaryFileStream::finishWriting()
{
    if (isWriteFinished())
        return stat;

    if (out_writer)
    {
        out_writer->finalize();
        /// The amount of written data can be changed after finalization, some buffers can be flushed
        /// Need to update the stat
        updateAllocAndCheck();
        out_writer.reset();

        /// reader will be created at the first read call, not to consume memory before it is needed
    }
    return stat;
}

TemporaryFileStream::Stat TemporaryFileStream::finishWritingAsyncSafe()
{
    std::call_once(finish_writing, [this]{ finishWriting(); });
    return stat;
}

bool TemporaryFileStream::isWriteFinished() const
{
    assert(in_reader == nullptr || out_writer == nullptr);
    return out_writer == nullptr;
}

Block TemporaryFileStream::read()
{
    if (!isWriteFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing has been not finished");

    if (isEof())
        return {};

    if (!in_reader)
    {
        in_reader = std::make_unique<Reader>(getPath(), header, getSize());
    }

    Block block = in_reader->read();
    if (!block)
    {
        /// finalize earlier to release resources, do not wait for the destructor
        this->release();
    }
    return block;
}

std::unique_ptr<TemporaryFileStream::Reader> TemporaryFileStream::getReadStream()
{
    if (!isWriteFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Writing has been not finished");

    if (isEof())
        return nullptr;

    return std::make_unique<Reader>(getPath(), header, getSize());
}

void TemporaryFileStream::updateAllocAndCheck()
{
    assert(out_writer);
    size_t new_compressed_size = out_writer->out_compressed_buf.getCompressedBytes();
    size_t new_uncompressed_size = out_writer->out_compressed_buf.getUncompressedBytes();

    if (unlikely(new_compressed_size < stat.compressed_size || new_uncompressed_size < stat.uncompressed_size))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Temporary file {} size decreased after write: compressed: {} -> {}, uncompressed: {} -> {}",
            getPath(), new_compressed_size, stat.compressed_size, new_uncompressed_size, stat.uncompressed_size);
    }

    parent->deltaAllocAndCheck(new_compressed_size - stat.compressed_size, new_uncompressed_size - stat.uncompressed_size);
    stat.compressed_size = new_compressed_size;
    stat.uncompressed_size = new_uncompressed_size;
    stat.num_rows = out_writer->num_rows;
}

bool TemporaryFileStream::isEof() const
{
    return file == nullptr && !segment_holder;
}

void TemporaryFileStream::release()
{
    if (in_reader)
        in_reader.reset();

    if (out_writer)
    {
        out_writer->finalize();
        out_writer.reset();
    }

    if (file)
    {
        file.reset();
        parent->deltaAllocAndCheck(-stat.compressed_size, -stat.uncompressed_size);
    }

    if (segment_holder)
        segment_holder.reset();
}

String TemporaryFileStream::getPath() const
{
    if (file)
        return file->getAbsolutePath();
    if (segment_holder && !segment_holder->empty())
        return segment_holder->front().getPath();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryFileStream has no file");
}

size_t TemporaryFileStream::getSize() const
{
    if (file)
        return file->getDisk()->getFileSize(file->getRelativePath());
    if (segment_holder && !segment_holder->empty())
        return segment_holder->front().getReservedSize();

    throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryFileStream has no file");
}

TemporaryFileStream::~TemporaryFileStream()
{
    try
    {
        release();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        assert(false); /// deltaAllocAndCheck with negative can't throw exception
    }
}

}
