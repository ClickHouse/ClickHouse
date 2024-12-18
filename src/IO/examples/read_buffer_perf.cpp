#include <string>

#include <iostream>

#include <base/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>


int main(int, char **)
{
    try
    {
        DB::ReadBufferFromFile in("test");

        Int64 a = 0;
        Float64 b = 0;
        String c, d;

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
