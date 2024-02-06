#include <iostream>
#include <Core/NamesAndTypes.h>
#include <IO/ReadBufferFromMemory.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    DB::ReadBufferFromMemory in(data, size);
    DB::NamesAndTypesList res;
    res.readText(in);

    return 0;
}
catch (...)
{
    return 1;
}
