#include <iostream>
#include <IO/ReadBufferFromMemory.h>
#include <Compression/CompressedReadBuffer.h>
#include <Common/Exception.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    DB::ReadBufferFromMemory from(data, size);
    DB::CompressedReadBuffer in{from};

    while (!in.eof())
        in.next();

    return 0;
}
catch (...)
{
    return 1;
}
