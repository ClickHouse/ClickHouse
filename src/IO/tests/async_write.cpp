#include <iostream>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/AsynchronousWriteBuffer.h>
#include <IO/copyData.h>
#include <Compression/CompressedWriteBuffer.h>


int main(int, char **)
try
{
    DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
    DB::WriteBufferFromFileDescriptor out1(STDOUT_FILENO);
    DB::AsynchronousWriteBuffer out2(out1);
    DB::CompressedWriteBuffer out3(out2);

    DB::copyData(in1, out3);

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
