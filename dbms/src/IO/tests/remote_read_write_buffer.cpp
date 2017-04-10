//#include <string>
#include <iostream>

#include <IO/RemoteReadBuffer.h>
#include <IO/RemoteWriteBuffer.h>

// Now just compile test

int main(int argc, char ** argv)
{
    try
    {
        DB::RemoteReadBuffer({}, {}, {});
        DB::RemoteWriteBuffer({}, {}, {});
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
