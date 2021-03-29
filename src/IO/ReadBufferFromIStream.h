#pragma once

#include <iostream>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#include <Core/UUID.h>

namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;
    bool nextImpl() override;

public:
    ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE);
    UUID uuid;
    off_t already_read = 0;
};

}
