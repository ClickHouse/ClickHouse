#pragma once

#include <memory>
#include <string>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{
struct ReadSettings;

/** Create an object to read data from a file.
  *
  * @param read_hint - the number of bytes to read hint
  * @param file_size - size of file
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename,
    const ReadSettings & settings,
    std::optional<size_t> read_hint = {},
    std::optional<size_t> file_size = {},
    int flags_ = -1,
    char * existing_memory = nullptr,
    size_t alignment = 0);
}
