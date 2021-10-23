#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <string>
#include <memory>


namespace DB
{

class MMappedFileCache;


/** Create an object to read data from a file.
  * estimated_size - the number of bytes to read
  * direct_io_threshold - the minimum number of bytes for asynchronous reads
  *
  * If direct_io_threshold = 0 or estimated_size < direct_io_threshold, read operations are executed synchronously.
  * Otherwise, the read operations are performed asynchronously.
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    size_t estimated_size,
    size_t direct_io_threshold,
    size_t mmap_threshold,
    MMappedFileCache * mmap_cache,
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    char * existing_memory = nullptr,
    size_t alignment = 0);

}
