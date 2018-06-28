#pragma once

#if !(defined(__FreeBSD__) || defined(__APPLE__) || defined(_MSC_VER))

#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Core/Defines.h>
#include <IO/AIO.h>
#include <Common/CurrentMetrics.h>

#include <string>
#include <unistd.h>
#include <fcntl.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForWrite;
}

namespace DB
{

/** Class for asynchronous data writing.
  */
class WriteBufferAIO : public WriteBufferFromFileBase
{
public:
    WriteBufferAIO(const std::string & filename_, size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE, int flags_ = -1, mode_t mode_ = 0666,
        char * existing_memory_ = nullptr);
    ~WriteBufferAIO() override;

    WriteBufferAIO(const WriteBufferAIO &) = delete;
    WriteBufferAIO & operator=(const WriteBufferAIO &) = delete;

    off_t getPositionInFile() override;
    void sync() override;
    std::string getFileName() const override { return filename; }
    int getFD() const override { return fd; }

private:
    void nextImpl() override;
    off_t doSeek(off_t off, int whence) override;
    void doTruncate(off_t length) override;

    /// If there's still data in the buffer, we'll write them.
    void flush();
    /// Wait for the end of the current asynchronous task.
    bool waitForAIOCompletion();
    /// Prepare an asynchronous request.
    void prepare();
    ///
    void finalize();

private:
    /// Buffer for asynchronous data writes.
    BufferWithOwnMemory<WriteBuffer> flush_buffer;

    /// Description of the asynchronous write request.
    iocb request{};
    iocb * request_ptr{&request};

    AIOContext aio_context{1};

    const std::string filename;

    /// The number of bytes to be written to the disk.
    off_t bytes_to_write = 0;
    /// Number of bytes written with the last request.
    off_t bytes_written = 0;
    /// The number of zero bytes to be cut from the end of the file
    /// after the data write operation completes.
    off_t truncation_count = 0;

    /// The current position in the file.
    off_t pos_in_file = 0;
    /// The maximum position reached in the file.
    off_t max_pos_in_file = 0;

    /// The starting position of the aligned region of the disk to which the data is written.
    off_t region_aligned_begin = 0;
    /// The size of the aligned region of the disk.
    size_t region_aligned_size = 0;

    /// The file descriptor for writing.
    int fd = -1;

    /// The data buffer that we want to write to the disk.
    Position buffer_begin = nullptr;

    /// Is the asynchronous write operation still in progress?
    bool is_pending_write = false;
    /// Did the asynchronous operation fail?
    bool aio_failed = false;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};
};

}

#endif
