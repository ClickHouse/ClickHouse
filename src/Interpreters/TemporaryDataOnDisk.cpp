#include <Interpreters/TemporaryDataOnDisk.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

void TemporaryDataOnDisk::deltaAlloc(int comp_delta, int uncomp_delta)
{
    size_t current_consuption = stat.compressed_size.fetch_add(comp_delta) + comp_delta;
    stat.uncompressed_size.fetch_add(uncomp_delta);

    if (parent)
        parent->deltaAlloc(comp_delta, uncomp_delta);

    if (comp_delta > 0 && limit && current_consuption > limit)
        throw Exception("Memory limit exceeded", ErrorCodes::MEMORY_LIMIT_EXCEEDED);
}

TemporaryFileStream & TemporaryDataOnDisk::createStream(const Block & header, CurrentMetrics::Value metric_scope, size_t reserve_size)
{
    std::lock_guard lock(mutex);

    DiskPtr disk = nullptr;
    ReservationPtr reservation = nullptr;
    if (reserve_size > 0)
    {
        reservation = volume->reserve(reserve_size);
        if (!reservation)
            throw Exception("Not enough space on temporary disk", ErrorCodes::NOT_ENOUGH_SPACE);
        disk = reservation->getDisk();
    }
    else
    {
        disk = volume->getDisk();
    }

    auto tmp_file = std::make_unique<TemporaryFileOnDisk>(disk, metric_scope);
    auto & tmp_stream = streams.emplace_back(std::make_unique<TemporaryFileStream>(std::move(tmp_file), header, this));
    return *tmp_stream;
}


struct TemporaryFileStream::OutputWriter
{
    OutputWriter(const String & path, const Block & header)
        : out_file_buf(path)
        , out_compressed_buf(out_file_buf)
        , out_writer(out_compressed_buf, DBMS_TCP_PROTOCOL_VERSION, header)
    {
    }

    void write(const Block & block)
    {
        if (finalized)
            throw Exception("Cannot write to finalized stream", ErrorCodes::LOGICAL_ERROR);
        out_writer.write(block);
    }


    void finalize()
    {
        if (finalized)
            return;
        out_writer.flush();
        out_compressed_buf.next();
        out_file_buf.next();
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

    bool finalized = false;
};

struct TemporaryFileStream::InputReader
{
    InputReader(const String & path, const Block & header)
        : in_file_buf(path)
        , in_compressed_buf(in_file_buf)
        , in_reader(in_compressed_buf, header, DBMS_TCP_PROTOCOL_VERSION)
    {}

    Block read() { return in_reader.read(); }

    ReadBufferFromFile in_file_buf;
    CompressedReadBuffer in_compressed_buf;
    NativeReader in_reader;
};

TemporaryFileStream::TemporaryFileStream(TemporaryFileOnDiskHolder file_, const Block & header, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , file(std::move(file_))
    , out_writer(std::make_unique<OutputWriter>(file->path(), header))
{
}

void TemporaryFileStream::write(const Block & block)
{
    if (!out_writer)
        throw Exception("Writing has been finished", ErrorCodes::LOGICAL_ERROR);

    out_writer->write(block);
    updateAlloc();
}

TemporaryFileStream::Stat TemporaryFileStream::finishWriting()
{
    if (out_writer)
    {
        Block header = out_writer->out_writer.getHeader();
        out_writer->finalize();
        updateAlloc();
        out_writer.reset();

        in_reader = std::make_unique<InputReader>(file->path(), header);
    }
    return {compressed_size, uncompressed_size};
}

bool TemporaryFileStream::isWriteFinished() const
{
    assert((out_writer || in_reader) && (!out_writer || !in_reader));
    return out_writer == nullptr;
}

Block TemporaryFileStream::read()
{
    if (!in_reader)
        throw Exception("Writing has been not finished", ErrorCodes::LOGICAL_ERROR);

    return in_reader->read();
}

Block TemporaryFileStream::getHeader() const
{
    if (in_reader)
        return in_reader->in_reader.getHeader();
    if (out_writer)
        return out_writer->out_writer.getHeader();
    return {};
}


void TemporaryFileStream::updateAlloc()
{
    size_t new_compressed_size = out_writer->out_compressed_buf.getCompressedBytes();
    size_t new_uncompressed_size = out_writer->out_compressed_buf.getUncompressedBytes();

    if (unlikely(new_compressed_size < compressed_size || new_uncompressed_size < uncompressed_size))
    {
        LOG_ERROR(&Poco::Logger::get("TemporaryFileStream"),
            "Temporary file {} size decreased after write: compressed: {} -> {}, uncompressed: {} -> {}",
            file->path(), new_compressed_size, compressed_size, new_uncompressed_size, uncompressed_size);
    }

    parent->deltaAlloc(new_compressed_size - compressed_size, new_uncompressed_size - uncompressed_size);
    this->compressed_size = new_compressed_size;
    this->uncompressed_size = new_uncompressed_size;
}

TemporaryFileStream::~TemporaryFileStream()
{
    try
    {
        parent->deltaAlloc(-compressed_size, -uncompressed_size);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
