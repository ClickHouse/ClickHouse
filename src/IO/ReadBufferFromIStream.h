#pragma once

#include <iostream>

#include "src/IO/ReadBuffer.h"
#include "src/IO/BufferWithOwnMemory.h"


namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;

    bool nextImpl() override;

public:
    ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE);
};

}
