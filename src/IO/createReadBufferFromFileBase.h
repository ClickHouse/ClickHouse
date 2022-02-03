#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <string>
#include <memory>


namespace DB
{

/** Create an object to read data from a file.
  *
  * @param size - the number of bytes to read
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    const ReadSettings & settings,
    std::optional<size_t> size = {},
    int flags_ = -1,
    char * existing_memory = nullptr,
    size_t alignment = 0);

}
