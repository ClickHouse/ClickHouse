#pragma once

#include <iostream>

#include <Common/Exception.h>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}


class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;

    bool nextImpl() override
    {
        istr.read(internal_buffer.begin(), internal_buffer.size());
        size_t gcount = istr.gcount();

        if (!gcount)
        {
            if (istr.eof())
                return false;
            else
                throw Exception("Cannot read from istream", ErrorCodes::CANNOT_READ_FROM_ISTREAM);
        }
        else
            working_buffer.resize(gcount);

        return true;
    }

public:
    ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_) {}
};

}
