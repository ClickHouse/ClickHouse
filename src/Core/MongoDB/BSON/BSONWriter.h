#pragma once
#include <IO/WriteBuffer.h>

namespace DB
{
namespace BSON
{


class BSONWriter
/// Class for writing BSON using a Poco::BinaryWriter.
{
public:
    explicit BSONWriter(WriteBuffer & writer_) : writer(writer_)
    /// Creates the BSONWriter.
    {
    }

    virtual ~BSONWriter() = default;

    template <typename T>
    void write(const T & t)
    {
        // default
        writer.write(reinterpret_cast<const char *>(&t), sizeof(t));
    }

    template <typename T>
    static Int32 getLength(const T & t);

private:
    WriteBuffer & writer;
};


}
}
