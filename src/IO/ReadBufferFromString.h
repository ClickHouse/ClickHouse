#pragma once

#include <IO/ReadBufferFromMemory.h>

namespace DB
{

/// Allows to read from std::string-like object.
class ReadBufferFromString : public ReadBufferFromMemory
{
public:
    /// std::string or something similar
    template <typename S>
    explicit ReadBufferFromString(const S & s) : ReadBufferFromMemory(s.data(), s.size()) {}

    explicit ReadBufferFromString(std::string_view s) : ReadBufferFromMemory(s.data(), s.size()) {}
};
}
