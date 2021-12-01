#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <string>
#include <memory>


namespace DB
{

/** Create an object to read data from a file.
  * estimated_size - the number of bytes to read
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    const ReadSettings & settings,
    size_t estimated_size,
    int flags_ = -1,
    char * existing_memory = nullptr,
    size_t alignment = 0);

}
