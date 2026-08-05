#pragma once

#include <sys/types.h>

#include <Common/CurrentMetrics.h>
#include <Common/IThrottler.h>
#include <IO/WriteBufferFromFileDescriptor.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForWrite;
}


#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{

/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class WriteBufferFromFile : public WriteBufferFromFileDescriptor
{
protected:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};

public:
    explicit WriteBufferFromFile(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        ThrottlerPtr throttler_ = {},
        mode_t mode = 0666,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        bool use_adaptive_buffer_size_ = false,
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);

    /// Use pre-opened file descriptor.
    explicit WriteBufferFromFile(
        int & fd,   /// Will be set to -1 if constructor didn't throw and ownership of file descriptor is passed to the object.
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        ThrottlerPtr throttler_ = {},
        char * existing_memory = nullptr,
        size_t alignment = 0,
        bool use_adaptive_buffer_size_ = false,
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);

    ~WriteBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override
    {
        return file_name;
    }

private:
    void finalizeImpl() override;
};

}
