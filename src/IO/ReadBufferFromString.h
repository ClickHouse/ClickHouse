#pragma once

#include <IO/ReadBufferFromMemory.h>


namespace DB
{

/** Allows to read from std::string-like object.
  */
class ReadBufferFromString : public ReadBufferFromMemory
{
public:
    /// std::string or something similar
    template <typename S>
    ReadBufferFromString(const S & s) : ReadBufferFromMemory(s.data(), s.size()) {}
};

}
