#include <string>

#include <iostream>
#include <fstream>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>


int main(int, char **)
{
    try
    {
        DB::ReadBufferFromFile in("DevicePixelRatio");

        DB::Float32 b = 0;

        size_t i = 0;
        while (!in.eof())
        {
            DB::readFloatText(b, in);
            in.ignore();

            ++i;
        }

        std::cout << b << std::endl;
        std::cout << i << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
