#pragma once

#include <iostream>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class WriteBufferFromOStream : public BufferWithOwnMemory<WriteBuffer>
{
protected:
    std::ostream * ostr;

    void nextImpl() override;

    WriteBufferFromOStream(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0);

public:
    WriteBufferFromOStream(
        std::ostream & ostr_,
        size_t size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromOStream() override;
};

}
