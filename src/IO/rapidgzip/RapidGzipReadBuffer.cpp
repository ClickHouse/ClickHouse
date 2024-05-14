#include <IO/rapidgzip/RapidGzipReadBuffer.h>
#include <ParallelGzipReader.hpp>
#include <memory>
#include <IO/CompressedReadBufferWrapper.h>

namespace rgzip = rapidgzip;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BZIP2_STREAM_DECODER_FAILED;
}

namespace
{

class ReadBufferRapidGzipAdapter : public FileReader
{
public:
    explicit ReadBufferRapidGzipAdapter(ReadBuffer * read_buf_)
        : read_buf(read_buf_)
    {
    }

    [[nodiscard]] UniqueFileReader clone() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cloning is not supported for ReadBufferRapidGzipAdapter");
    }

    void close() override
    {
        read_buf = nullptr;
    }

    [[nodiscard]] bool closed() const override
    {
        return !read_buf;
    }

    [[nodiscard]] bool eof() const override
    {
        return read_buf->eof();
    }

    [[nodiscard]] bool fail() const override
    {
        // read_buf does not have a fail state; returning false
        return false;
    }

    [[nodiscard]] int fileno() const override
    {
        return -1;
    }

    [[nodiscard]] bool seekable() const override
    {
        return false;
    }

    size_t read(char * buffer, size_t nMaxBytesToRead) override
    {
        return read_buf->read(buffer, nMaxBytesToRead);
    }

    size_t seek(long long int offset, int origin = SEEK_SET) override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Seek operation is not supported for ReadBufferRapidGzipAdapter");
    }

    [[nodiscard]] std::optional<size_t> size() const override
    {
        return std::nullopt;
    }

    [[nodiscard]] size_t tell() const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tell operation is not supported for ReadBufferRapidGzipAdapter");
    }

    void clearerr() override
    {
    }

private:
    ReadBuffer * read_buf;
};

}

class RapidGzipReadBufferImpl
{
public:
    explicit RapidGzipReadBufferImpl(ReadBuffer * in_)
        : rapid_gzip_reader(std::make_unique<ReadBufferRapidGzipAdapter>(in_))
        , in_available(0)
        , in_data(nullptr)
        , out_capacity(0)
        , out_data(nullptr)
        , eof_flag(false)
    {
    }

    rgzip::ParallelGzipReader<rgzip::ChunkData> rapid_gzip_reader;

    size_t in_available;
    char * in_data;
    size_t out_capacity;
    char * out_data;
    bool eof_flag;
};

RapidGzipReadBuffer::RapidGzipReadBuffer(
    std::unique_ptr<ReadBuffer> in_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , impl(std::make_unique<RapidGzipReadBufferImpl>(in.get()))
{
}

RapidGzipReadBuffer::~RapidGzipReadBuffer() = default;

bool RapidGzipReadBuffer::nextImpl()
{
    if (impl->eof_flag)
        return false;

    char * output_buffer = internal_buffer.begin();
    size_t buffer_size = internal_buffer.size();

    try
    {
        size_t bytes_read = impl->rapid_gzip_reader.read(output_buffer, buffer_size);
        if (bytes_read == 0)
        {
            impl->eof_flag = true;
            return false;
        }

        working_buffer.resize(bytes_read);
        return true;
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::BZIP2_STREAM_DECODER_FAILED, "Failed to read from gzip stream: {}", e.what());
    }
}

}
