#include <string>

#include <iostream>

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <base/types.h>

int readAndPrint(DB::ReadBuffer & in)
{
    try
    {
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
        return 0;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }
}

int main(int, char **)
{
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        DB::ReadBufferFromString in(s);
        if (readAndPrint(in))
            std::cout << "readAndPrint from ReadBufferFromString failed" << std::endl;
    }


    std::shared_ptr<DB::ReadBufferFromOwnString> in;
    {
        std::string s = "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
        in = std::make_shared<DB::ReadBufferFromOwnString>(s);
    }
    if (readAndPrint(*in))
        std::cout << "readAndPrint from ReadBufferFromOwnString failed" << std::endl;

    return 0;
}
