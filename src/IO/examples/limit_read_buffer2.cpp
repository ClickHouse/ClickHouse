#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <iostream>
#include <sstream>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}


int main(int, char **)
try
{
    using namespace DB;

    {
        std::string src = "1";

        std::string dst;

        ReadBuffer in(src.data(), src.size(), 0);

        auto limit_in = LimitReadBuffer(in, {.read_no_more = 1});

        {
            WriteBufferFromString out(dst);

            copyData(limit_in, out);
        }

        if (limit_in.count() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, incorrect count(): {}", limit_in.count());

        if (in.count() != limit_in.count())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, incorrect underlying buffer's count(): {}", in.count());
        if (src != dst)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, incorrect destination value, read: {}, expected: {}", dst, src);
    }
    {
        std::string src = "abc";
        ReadBuffer in(src.data(), src.size(), 0);

        std::string dst;

        {
            WriteBufferFromString out(dst);

            char x;
            readChar(x, in);

            auto limit_in = LimitReadBuffer(in, {.read_no_more = 1});

            copyData(limit_in, out);


            if (in.count() != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect underlying buffer's count: {}, expected: {}", in.count(), 2);

            if (limit_in.count() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect count: {}, expected: {}", limit_in.count(), 1);
        }

        if (dst != "b")
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect destination value: {}, expected 'b'", dst);

        char y;
        readChar(y, in);
        if (y != 'c')
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Read incorrect value from underlying buffer: {}, expected 'c'", y);
        while (!in.eof())
            in.ignore();
        if (in.count() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect final count from underlying buffer: {}, expected: 3", in.count());
    }

    {
        std::string src = "abc";
        ReadBuffer in(src.data(), src.size(), 0);

        {
            auto limit_in = LimitReadBuffer(in, {.read_no_more = 1});

            char x;
            readChar(x, limit_in);

            if (limit_in.count() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect count: {}, expected: {}", limit_in.count(), 1);
        }

        if (in.count() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed!, Incorrect final count from underlying buffer: {}, expected: 1", in.count());
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
