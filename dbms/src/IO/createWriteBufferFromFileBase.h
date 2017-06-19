#pragma once

#include <IO/WriteBufferFromFileBase.h>
#include <string>

namespace DB
{

/** Create an object to write data to a file.
  * estimated_size - number of bytes to write
  * aio_threshold - the minimum number of bytes for asynchronous writes
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, the write operations are executed synchronously.
  * Otherwise, write operations are performed asynchronously.
  */
WriteBufferFromFileBase * createWriteBufferFromFileBase(const std::string & filename_,
        size_t estimated_size,
        size_t aio_threshold,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags_ = -1,
        mode_t mode = 0666,
        char * existing_memory_ = nullptr,
        size_t alignment = 0);

}
