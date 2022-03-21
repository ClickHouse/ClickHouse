#include <string>

#include <iostream>

#include <base/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>


int main(int, char **)
{
    try
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        DB::ReadBufferFromString in(s);

        DB::Int64 a;
        DB::Float64 b;
        DB::String c, d;

        DB::readIntText(a, in);
        in.ignore();

        DB::readFloatText(b, in);
        in.ignore();

        DB::readEscapedString(c, in);
        in.ignore();

        DB::readQuotedString(d, in);

        std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
        std::cout << in.count() << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
