#pragma once

#include <iostream>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class WriteBufferFromOStream : public BufferWithOwnMemory<WriteBuffer>
{
public:
    WriteBufferFromOStream(
        std::ostream & ostr_,
        size_t size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromOStream() override;

protected:
    WriteBufferFromOStream(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0);

    void nextImpl() override;

    std::ostream * ostr{};
};

}
