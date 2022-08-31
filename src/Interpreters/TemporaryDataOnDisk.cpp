#include <Interpreters/TemporaryDataOnDisk.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

void TemporaryDataOnDisk::deltaAlloc(int compressed_size, int uncompressed_size)
{
    size_t current_consuption = stat.compressed_size.fetch_add(compressed_size) + compressed_size;
    stat.uncompressed_size.fetch_add(uncompressed_size);

    if (parent)
        parent->deltaAlloc(comp_delta, uncomp_delta);

    if (compressed_size > 0 && limit && current_consuption > limit)
        throw Exception("Memory limit exceeded", ErrorCodes::MEMORY_LIMIT_EXCEEDED);
}

void TemporaryDataOnDisk::setAlloc(size_t compressed_size, size_t uncompressed_size)
{
    int comp_delta = compressed_size - stat.compressed_size.exchange(compressed_size);
    int uncomp_delta = uncompressed_size - stat.uncompressed_size.exchange(uncompressed_size);

    if (parent)
        parent->deltaAlloc(comp_delta, uncomp_delta);
}

TemporaryFileStream & TemporaryDataOnDisk::createStream(CurrentMetrics::Value metric_scope, size_t reserve_size)
{
    std::lock_guard lock(mutex);

    DiskPtr disk = nullptr;
    ReservationPtr reservation = nullptr;
    if (reserve_size > 0)
    {
        reservation = tmp_volume->reserve(reserve_size);
        if (!reservation)
            throw Exception("Not enough space on temporary disk", ErrorCodes::NOT_ENOUGH_SPACE);
        disk = reservation->getDisk();
    }
    else
    {
        disk = volume->getDisk();
    }

    TemporaryFileOnDiskHolder tmp_file = std::make_unique<TemporaryFileOnDisk>(disk, metric_scope);
    TemporaryFileStreamHolder & tmp_stream = streams.emplace_back(std::make_unique<TemporaryFileStream>(tmp_file, this));
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
        out_writer.finalize();
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

TemporaryFileStream::TemporaryFileStream(TemporaryFileOnDiskHolder file_, const TemporaryDataOnDiskPtr & parent_)
    : parent(parent_)
    , file(file_)
    , out_writer(std::make_unique<OutputWriter>(file->path(), header))
{
}

void TemporaryFileStream::write(const Block & block)
{
    if (!out_writer)
        throw Exception("Writing has beed finished", ErrorCodes::LOGICAL_ERROR);

    out_writer->write(block);
    setAlloc(out_writer->out_compressed_buf.getCompressedBytes(), out_writer->out_compressed_buf.getUncompressedBytes());
}

Stat TemporaryFileStream::finishWriting()
{
    if (out_writer)
    {
        out_writer->finalize();
        setAlloc(out_writer->out_compressed_buf.getCompressedBytes(), out_writer->out_compressed_buf.getUncompressedBytes());

        in_reader = std::make_unique<InputReader>(file->path(), header);
    }
    return stat;
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

~TemporaryFileStream::TemporaryFileStream()
{
    try
    {
        setAlloc(0, 0);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
