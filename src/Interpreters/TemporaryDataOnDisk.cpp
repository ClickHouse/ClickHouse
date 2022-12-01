#include <Interpreters/TemporaryDataOnDisk.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
#include <Disks/TemporaryFileInPath.h>

#include <Common/logger_useful.h>

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

VolumePtr TemporaryDataOnDiskScope::getVolume() const
{
    if (!volume)
        throw Exception("TemporaryDataOnDiskScope has no volume", ErrorCodes::LOGICAL_ERROR);
    return volume;
}

TemporaryFileStream & TemporaryDataOnDisk::createStream(const Block & header, size_t max_file_size)
{
    TemporaryFileStreamPtr tmp_stream;
    if (cache)
        tmp_stream = TemporaryFileStream::create(cache, header, max_file_size, this);
    else
        tmp_stream = TemporaryFileStream::create(volume, header, max_file_size, this);

    std::lock_guard lock(mutex);
    return *streams.emplace_back(std::move(tmp_stream));
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
    OutputWriter(const String & path, const Block & header_)
        : out_file_buf(path)
        , out_compressed_buf(out_file_buf)
        , out_writer(out_compressed_buf, DBMS_TCP_PROTOCOL_VERSION, header_)
    {
    }

    size_t write(const Block & block)
    {
        if (finalized)
            throw Exception("Cannot write to finalized stream", ErrorCodes::LOGICAL_ERROR);
        size_t written_bytes = out_writer.write(block);
        num_rows += block.rows();
        return written_bytes;
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
        out_file_buf.finalize();
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

    WriteBufferFromFile out_file_buf;
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
    }

    explicit InputReader(const String & path)
        : in_file_buf(path)
        , in_compressed_buf(in_file_buf)
        , in_reader(in_compressed_buf, DBMS_TCP_PROTOCOL_VERSION)
    {
    }

    Block read() { return in_reader.read(); }

    ReadBufferFromFile in_file_buf;
    CompressedReadBuffer in_compressed_buf;
    NativeReader in_reader;
};

TemporaryFileStreamPtr TemporaryFileStream::create(const VolumePtr & volume, const Block & header, size_t max_file_size, TemporaryDataOnDisk * parent_)
{
    if (!volume)
        throw Exception("TemporaryDataOnDiskScope has no volume", ErrorCodes::LOGICAL_ERROR);

    DiskPtr disk;
    if (max_file_size > 0)
    {
        auto reservation = volume->reserve(max_file_size);
        if (!reservation)
            throw Exception("Not enough space on temporary disk", ErrorCodes::NOT_ENOUGH_SPACE);
        disk = reservation->getDisk();
    }
    else
    {
        disk = volume->getDisk();
    }

    auto tmp_file = std::make_unique<TemporaryFileOnDisk>(disk, parent_->getMetricScope());
    return std::make_unique<TemporaryFileStream>(std::move(tmp_file), header, /* cache_placeholder */ nullptr, /* parent */ parent_);
}

TemporaryFileStreamPtr TemporaryFileStream::create(FileCache * cache, const Block & header, size_t max_file_size, TemporaryDataOnDisk * parent_)
{
    auto tmp_file = std::make_unique<TemporaryFileInPath>(fs::path(cache->getBasePath()) / "tmp");

    auto cache_placeholder = std::make_unique<FileCachePlaceholder>(cache, tmp_file->getPath());
    cache_placeholder->reserveCapacity(max_file_size);

    return std::make_unique<TemporaryFileStream>(std::move(tmp_file), header, std::move(cache_placeholder), parent_);
}

TemporaryFileStream::TemporaryFileStream(
    TemporaryFileHolder file_,
    const Block & header_,
    std::unique_ptr<ISpacePlaceholder> space_holder_,
    TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , header(header_)
    , file(std::move(file_))
    , space_holder(std::move(space_holder_))
    , out_writer(std::make_unique<OutputWriter>(file->getPath(), header))
{
}

size_t TemporaryFileStream::write(const Block & block)
{
    if (!out_writer)
        throw Exception("Writing has been finished", ErrorCodes::LOGICAL_ERROR);

    size_t block_size_in_memory = block.bytes();

    if (space_holder)
        space_holder->reserveCapacity(block_size_in_memory);

    updateAllocAndCheck();

    size_t bytes_written = out_writer->write(block);
    if (space_holder)
        space_holder->setUsed(bytes_written);

    return bytes_written;
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
        in_reader = std::make_unique<InputReader>(file->getPath(), header);
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
            file->getPath(), new_compressed_size, stat.compressed_size, new_uncompressed_size, stat.uncompressed_size);
    }

    parent->deltaAllocAndCheck(new_compressed_size - stat.compressed_size, new_uncompressed_size - stat.uncompressed_size);
    stat.compressed_size = new_compressed_size;
    stat.uncompressed_size = new_uncompressed_size;
    stat.num_rows = out_writer->num_rows;
}

bool TemporaryFileStream::isEof() const
{
    return file == nullptr;
}

void TemporaryFileStream::release()
{
    if (file)
    {
        file.reset();
        parent->deltaAllocAndCheck(-stat.compressed_size, -stat.uncompressed_size);
    }

    if (in_reader)
        in_reader.reset();

    if (out_writer)
    {
        out_writer->finalize();
        out_writer.reset();
    }
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
