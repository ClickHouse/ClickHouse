#include <iostream>

#include <IO/RemoteReadBuffer.h>
#include <IO/RemoteWriteBuffer.h>


int main(int, char **)
try
{
    DB::RemoteReadBuffer({}, {}, {});
    DB::RemoteWriteBuffer({}, {}, {});

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
