#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/AsynchronousReader.h>
#include <Interpreters/Context.h>

#include <optional>
#include <unistd.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class AsynchronousReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
protected:
    AsynchronousReaderPtr reader;
    Int32 priority;

    Memory<> prefetch_buffer;
    std::future<IAsynchronousReader::Result> prefetch_future;

    const size_t required_alignment = 0;  /// For O_DIRECT both file offsets and memory addresses have to be aligned.
    size_t file_offset_of_buffer_end = 0; /// What offset in file corresponds to working_buffer.end().
    size_t bytes_to_ignore = 0;           /// How many bytes should we ignore upon a new read request.
    int fd;

    bool nextImpl() override;

    /// Name or some description of file.
    std::string getFileName() const override;

    void finalize();

public:
    AsynchronousReadBufferFromFileDescriptor(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        int fd_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ~AsynchronousReadBufferFromFileDescriptor() override;

    void prefetch() override;

    int getFD() const
    {
        return fd;
    }

    off_t getPosition() override
    {
        return file_offset_of_buffer_end - (working_buffer.end() - pos);
    }

    /// If 'offset' is small enough to stay in buffer after seek, then true seek in file does not happen.
    off_t seek(off_t off, int whence) override;

    /// Seek to the beginning, discarding already read data if any. Useful to reread file that changes on every read.
    void rewind();

private:
    std::future<IAsynchronousReader::Result> asyncReadInto(char * data, size_t size);
};

}

