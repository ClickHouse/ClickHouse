#pragma once
#include "config_formats.h"

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
    RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_, off_t file_size_);

    explicit RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_);

    arrow::Result<int64_t> GetSize() override;

    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;

    bool closed() const override { return !is_open; }

    arrow::Result<int64_t> Read(int64_t nbytes, void * out) override;

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

    arrow::Status Seek(int64_t position) override;

private:
    ReadBuffer & in;
    SeekableReadBuffer & seekable_in;
    std::optional<off_t> file_size;
    bool is_open = false;

    ARROW_DISALLOW_COPY_AND_ASSIGN(RandomAccessFileFromSeekableReadBuffer);
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
    const std::string & magic_bytes);

}

#endif
