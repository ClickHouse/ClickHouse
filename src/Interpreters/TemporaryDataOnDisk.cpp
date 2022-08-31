#include <Interpreters/TemporaryDataOnDisk.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

void TemporaryDataOnDisk::changeAlloc(size_t compressed_bytes, size_t uncompressed_bytes, bool is_dealloc)
{
    if (is_dealloc)
    {
        stat.uncompressed_bytes -= uncompressed_bytes;
        stat.compressed_bytes -= compressed_bytes;
    }
    else
    {
        stat.uncompressed_bytes += uncompressed_bytes;
        stat.compressed_bytes += compressed_bytes;
    }

    if (parent)
        parent->changeAlloc(compressed_bytes, uncompressed_bytes, is_dealloc);

    if (limit && !is_dealloc)
    {
        if (stat.compressed_bytes > limit)
            throw Exception("Memory limit exceeded", ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
}

TemporaryFileStream::TemporaryFileStream(TemporaryFileOnDiskHolder file_, TemporaryDataOnDisk * parent_)
    : parent(parent_)
    , file(file_)
    , out_writer(std::make_unique<OutputWriter>(file->path(), header))
{
}

TemporaryFileStreamPtr TemporaryDataOnDisk::createStream(CurrentMetrics::Value metric_scope, size_t max_temp_file_size)
{
    DiskPtr disk = nullptr;
    ReservationPtr reservation = nullptr;
    if (max_temp_file_size > 0)
    {
        reservation = tmp_volume->reserve(size);
        if (!reservation)
            throw Exception("Not enough space on temporary disk", ErrorCodes::NOT_ENOUGH_SPACE);
        disk = reservation->getDisk();
    }
    else
    {
        disk = volume->getDisk();
    }

    TemporaryFileOnDiskHolder tmp_file = std::make_unique<TemporaryFileOnDisk>(disk, metric_scope);
    return std::make_unique<TemporaryFileStream>(tmp_file, this);
}


~TemporaryFileStream::TemporaryFileStream()
{
    try
    {
        dealloc(stat.compressed_bytes, stat.uncompressed_bytes);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
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
    {
    }

    ReadBufferFromFile in_file_buf;
    CompressedReadBuffer in_compressed_buf;
    NativeReader in_reader;
};

}
