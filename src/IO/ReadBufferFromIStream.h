#pragma once

#include <iostream>

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;

    bool nextImpl() override;

    size_t read_pos = 0;
    size_t offset = 0;
public:
    explicit ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE, size_t offset_ = 0);
};

}
