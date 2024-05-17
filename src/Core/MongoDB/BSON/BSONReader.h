#pragma once
#include <exception>
#include <IO/ReadBuffer.h>

namespace DB
{
namespace BSON
{


class BSONReader
/// Class for reading BSON using a ReadBuffer
{
public:
    explicit BSONReader(ReadBuffer & reader_) : reader(reader_)
    /// Creates the BSONReader using the given ReadBuffer.
    {
    }

    virtual ~BSONReader() = default;

    template <typename T>
    T read();

    std::string readCString();
    /// Reads a cstring from the reader.
    /// A cstring is a string terminated with a 0x00.

private:
    ReadBuffer & reader;
};


//
// inlines
//
inline std::string BSONReader::readCString()
{
    std::string val;
    while (!reader.eof())
    {
        char c;
        if (!reader.read(c))
            throw std::exception(); // FIXME

        if (c == 0x00)
            break;
        val += c;
    }
    return val;
}


}
}
