#include <iostream>

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    DB::ReadBufferFromMemory in(data, size);
    DB::MergeTreeDataPartChecksums res;
    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    SCOPE_EXIT(out.finalize());

    if (!res.read(in))
        return 1;
    res.write(out);

    return 0;
}
catch (...)
{
    return 1;
}
