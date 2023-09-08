#include <Interpreters/TemporaryDataOnDisk.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/WriteBufferFromTemporaryFile.h>

#include <Common/logger_useful.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>

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

    size_t new_consumprion = stat.compressed_size + compressed_delta;
    if (compressed_delta > 0 && limit && new_consumprion > limit)
        throw Exception(ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "Limit for temporary files size exceeded (would consume {} / {} bytes)", new_consumprion, limit);

    stat.compressed_size += compressed_delta;
    stat.uncompressed_size += uncompressed_delta;
}

TemporaryDataOnDisk::TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_)
    : TemporaryDataOnDiskScope(std::move(parent_), /* limit_ = */ 0)
{}

TemporaryDataOnDisk::TemporaryDataOnDisk(TemporaryDataOnDiskScopePtr parent_, CurrentMetrics::Metric metric_scope)
    : TemporaryDataOnDiskScope(std::move(parent_), /* limit_ = */ 0)
    , current_metric_scope(metric_scope)
{}

WriteBufferPtr TemporaryDataOnDisk::createRawStream(size_t max_file_size)
{
    if (file_cache)
    {
        auto holder = createCacheFile(max_file_size);
        return std::make_shared<WriteBufferToFileSegment>(std::move(holder));
    }
    else if (volume)
    {
        auto tmp_file = createRegularFile(max_file_size);
        return std::make_shared<WriteBufferFromTemporaryFile>(std::move(tmp_file));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryDataOnDiskScope has no cache and no volume");
}

TemporaryFileStream & TemporaryDataOnDisk::createStream(const Block & header, size_t max_file_size)
{
    if (file_cache)
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

    const auto key = FileSegment::Key::random();
    auto holder = file_cache->set(key, 0, std::max(10_MiB, max_file_size), CreateFileSegmentSettings(FileSegmentKind::Temporary, /* unbounded */ true));
    fs::create_directories(file_cache->getPathInLocalCache(key));
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

struct TemporaryFileStream::OutputWriter
{
    OutputWriter(std::unique_ptr<WriteBuffer> out_buf_, const Block & header_)
        : out_buf(std::move(out_buf_))
        , out_compressed_buf(*out_buf)
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

struct TemporaryFileStream::InputReader
{
    InputReader(const String & path, const Block & header_)
        : in_file_buf(path)
        , in_compressed_buf(in_file_buf)
        , in_reader(in_compressed_buf, header_, DBMS_TCP_PROTOCOL_VERSION)
    {
        LOG_TEST(&Poco::Logger::get("TemporaryFileStream"), "Reading {} from {}", header_.dumpStructure(), path);
    }

    explicit InputReader(const String & path)
        : in_file_buf(path)
        , in_compressed_buf(in_file_buf)
        , in_reader(in_compressed_buf, DBMS_TCP_PROTOCOL_VERSION)
    {
        LOG_TEST(&Poco::Logger::get("TemporaryFileStream"), "Reading from {}", path);
    }

    Block read()
    {
        return in_reader.read();
    }

    ReadBufferFromFile in_file_buf;
    CompressedReadBuffer in_compressed_buf;
    NativeReader in_reader;
};

TemporaryFileStream::TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header_, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , header(header_)
    , file(std::move(file_))
    , out_writer(std::make_unique<OutputWriter>(std::make_unique<WriteBufferFromFile>(file->getAbsolutePath()), header))
{
    LOG_TEST(&Poco::Logger::get("TemporaryFileStream"), "Writing to temporary file {}", file->getAbsolutePath());
}

TemporaryFileStream::TemporaryFileStream(FileSegmentsHolderPtr segments_, const Block & header_, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , header(header_)
    , segment_holder(std::move(segments_))
{
    if (segment_holder->size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TemporaryFileStream can be created only from single segment");
    auto out_buf = std::make_unique<WriteBufferToFileSegment>(&segment_holder->front());

    LOG_TEST(&Poco::Logger::get("TemporaryFileStream"), "Writing to temporary file {}", out_buf->getFileName());
    out_writer = std::make_unique<OutputWriter>(std::move(out_buf), header);
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
        in_reader = std::make_unique<InputReader>(getPath(), header);
    }

    Block block = in_reader->read();
    if (!block)
    {
        /// finalize earlier to release resources, do not wait for the destructor
        this->release();
    }
    return block;
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
        return segment_holder->front().getPathInLocalCache();

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
