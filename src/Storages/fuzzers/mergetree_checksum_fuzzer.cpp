
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        DB::ReadBufferFromMemory in(data, size);
        DB::MergeTreeDataPartChecksums res;
        DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

        if (!res.read(in))
            return 0;
        res.write(out);
    }
    catch (...)
    {
    }

    return 0;
}
