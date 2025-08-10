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

    std::string_view stringView() const { return std::string_view(pos, working_buffer.end() - pos); }
};

class ReadBufferFromOwnString : public String, public ReadBufferFromString
{
public:
    template <typename S>
    explicit ReadBufferFromOwnString(S && s_) : String(std::forward<S>(s_)), ReadBufferFromString(*this)
    {
    }
};

}
