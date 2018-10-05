#pragma once

#if defined(__linux__)

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/AIO.h>
#include <Core/Defines.h>
#include <Common/CurrentMetrics.h>
#include <string>
#include <limits>
#include <future>
#include <unistd.h>
#include <fcntl.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}

namespace DB
{

/** Class for asynchronous data reading.
  */
class ReadBufferAIO : public ReadBufferFromFileBase
{
public:
    ReadBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1,
        char * existing_memory_ = nullptr);
    ~ReadBufferAIO() override;

    ReadBufferAIO(const ReadBufferAIO &) = delete;
    ReadBufferAIO & operator=(const ReadBufferAIO &) = delete;

    void setMaxBytes(size_t max_bytes_read_);
    off_t getPositionInFile() override { return first_unread_pos_in_file - (working_buffer.end() - pos); }
    std::string getFileName() const override { return filename; }
    int getFD() const override { return fd; }

private:
    ///
    bool nextImpl() override;
    ///
    off_t doSeek(off_t off, int whence) override;
    /// Synchronously read the data.
    void synchronousRead();
    /// Get data from an asynchronous request.
    void receive();
    /// Ignore data from an asynchronous request.
    void skip();
    /// Wait for the end of the current asynchronous task.
    bool waitForAIOCompletion();
    /// Prepare the request.
    void prepare();
    /// Prepare for reading a duplicate buffer containing data from
    /// of the last request.
    void finalize();

private:
    /// Buffer for asynchronous data read operations.
    BufferWithOwnMemory<ReadBuffer> fill_buffer;

    /// Description of the asynchronous read request.
    iocb request{};
    std::future<ssize_t> future_bytes_read;

    const std::string filename;

    /// The maximum number of bytes that can be read.
    size_t max_bytes_read = std::numeric_limits<size_t>::max();
    /// Number of bytes requested.
    size_t requested_byte_count = 0;
    /// The number of bytes read at the last request.
    ssize_t bytes_read = 0;
    /// The total number of bytes read.
    size_t total_bytes_read = 0;

    /// The position of the first unread byte in the file.
    off_t first_unread_pos_in_file = 0;

    /// The starting position of the aligned region of the disk from which the data is read.
    off_t region_aligned_begin = 0;
    /// Left offset to align the region of the disk.
    size_t region_left_padding = 0;
    /// The size of the aligned region of the disk.
    size_t region_aligned_size = 0;

    /// The file descriptor for read.
    int fd = -1;

    /// The buffer to which the received data is written.
    Position buffer_begin = nullptr;

    /// The asynchronous read operation is not yet completed.
    bool is_pending_read = false;
    /// The end of the file is reached.
    bool is_eof = false;
    /// At least one read request was sent.
    bool is_started = false;
    /// Is the operation asynchronous?
    bool is_aio = false;
    /// Did the asynchronous operation fail?
    bool aio_failed = false;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};
};

}

#endif
