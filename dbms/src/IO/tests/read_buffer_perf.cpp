#include <string>

#include <iostream>
#include <fstream>

#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromIStream.h>


int main(int argc, char ** argv)
{
    try
    {
        std::ifstream istr("test");
        DB::ReadBufferFromIStream in(istr);

        DB::Int64 a = 0;
        DB::Float64 b = 0;
        DB::String c, d;

        size_t i = 0;
        while (!in.eof())
        {
            DB::readIntText(a, in);
            in.ignore();

            DB::readFloatText(b, in);
            in.ignore();

            DB::readEscapedString(c, in);
            in.ignore();

            DB::readQuotedString(d, in);
            in.ignore();

            ++i;
        }

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << i << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
