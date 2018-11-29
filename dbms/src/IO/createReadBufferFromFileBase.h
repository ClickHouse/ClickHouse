#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <string>
#include <memory>


namespace DB
{

/** Create an object to read data from a file.
  * estimated_size - the number of bytes to read
  * aio_threshold - the minimum number of bytes for asynchronous reads
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, read operations are executed synchronously.
  * Otherwise, the read operations are performed asynchronously.
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);

}
