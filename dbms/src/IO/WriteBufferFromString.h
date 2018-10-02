#pragma once

#include <string>

#include <IO/WriteBuffer.h>

#define WRITE_BUFFER_FROM_STRING_INITIAL_SIZE_IF_EMPTY 32


namespace DB
{

/** Writes the data to a string.
  * Note: before using the resulting string, destroy this object.
  */
class WriteBufferFromString : public WriteBuffer
{
private:
    std::string & s;

    void nextImpl() override
    {
        size_t old_size = s.size();
        s.resize(old_size * 2);
        internal_buffer = Buffer(reinterpret_cast<Position>(&s[old_size]), reinterpret_cast<Position>(&s[s.size()]));
        working_buffer = internal_buffer;
    }

protected:
    void finish()
    {
        s.resize(count());
    }

public:
    WriteBufferFromString(std::string & s_)
        : WriteBuffer(reinterpret_cast<Position>(s_.data()), s_.size()), s(s_)
    {
        if (s.empty())
        {
            s.resize(WRITE_BUFFER_FROM_STRING_INITIAL_SIZE_IF_EMPTY);
            set(reinterpret_cast<Position>(s.data()), s.size());
        }
    }

    ~WriteBufferFromString() override
    {
        finish();
    }
};


namespace detail
{
    /// For correct order of initialization.
    class StringHolder
    {
    protected:
        std::string value;
    };
}

/// Creates the string by itself and allows to get it.
class WriteBufferFromOwnString : public detail::StringHolder, public WriteBufferFromString
{
public:
    WriteBufferFromOwnString() : WriteBufferFromString(value) {}

    std::string & str()
    {
        finish();
        return value;
    }
};

}
