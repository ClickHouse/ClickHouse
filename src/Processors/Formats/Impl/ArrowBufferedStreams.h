#pragma once
#include "config.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <optional>

#include <arrow/io/interfaces.h>

#define ORC_MAGIC_BYTES "ORC"
#define PARQUET_MAGIC_BYTES "PAR1"
#define ARROW_MAGIC_BYTES "ARROW1"

namespace DB
{

class ReadBuffer;
class WriteBuffer;

class SeekableReadBuffer;
struct FormatSettings;

class ArrowBufferedOutputStream : public arrow::io::OutputStream
{
public:
    explicit ArrowBufferedOutputStream(WriteBuffer & out_);

    // FileInterface
    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    // Writable
    arrow::Status Write(const void * data, int64_t length) override;

private:
    WriteBuffer & out;
    int64_t total_length = 0;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowBufferedOutputStream);
};

class RandomAccessFileFromSeekableReadBuffer : public arrow::io::RandomAccessFile
{
public:
    RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_, std::optional<off_t> file_size_, bool avoid_buffering_);

    arrow::Result<int64_t> GetSize() override;

    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    arrow::Result<int64_t> Read(int64_t nbytes, void * out) override;

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    /// Override async reading to avoid using internal arrow thread pool.
    /// In our code we don't use async reading, so implementation is sync,
    /// we just call ReadAt and return future with ready value.
    arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext&, int64_t position, int64_t nbytes) override;

    arrow::Status Seek(int64_t position) override;

private:
    ReadBuffer & in;
    SeekableReadBuffer & seekable_in;
    std::optional<off_t> file_size;
    bool is_open = false;
    bool avoid_buffering = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromSeekableReadBuffer);
};

class RandomAccessFileFromRandomAccessReadBuffer : public arrow::io::RandomAccessFile
{
public:
    explicit RandomAccessFileFromRandomAccessReadBuffer(SeekableReadBuffer & in_, size_t file_size_);

    // These are thread safe.
    arrow::Result<int64_t> GetSize() override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
    arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(
        const arrow::io::IOContext&, int64_t position, int64_t nbytes) override;

    // These are not thread safe, and arrow shouldn't call them. Return NotImplemented error.
    arrow::Status Seek(int64_t) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Result<int64_t> Read(int64_t, void*) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t) override;

    arrow::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    SeekableReadBuffer & in;
    size_t file_size;
    bool is_open = true;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromRandomAccessReadBuffer);
};

class ArrowInputStreamFromReadBuffer : public arrow::io::InputStream
{
public:
    explicit ArrowInputStreamFromReadBuffer(ReadBuffer & in);
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Status Abort() override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override { return !is_open; }

private:
    ReadBuffer & in;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowInputStreamFromReadBuffer);
};

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes,
    // If true, we'll use ReadBuffer::setReadUntilPosition() to avoid buffering and readahead as
    // much as possible. For HTTP or S3 ReadBuffer, this means that each RandomAccessFile
    // read call will do a new HTTP request. Used in parquet pre-buffered reading mode, which makes
    // arrow do its own buffering and coalescing of reads.
    // (ReadBuffer is not a good abstraction in this case, but it works.)
    bool avoid_buffering = false);

// Reads the whole file into a memory buffer, owned by the returned RandomAccessFile.
std::shared_ptr<arrow::io::RandomAccessFile> asArrowFileLoadIntoMemory(
    ReadBuffer & in,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes);

}

#endif
