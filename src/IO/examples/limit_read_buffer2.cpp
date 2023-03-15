#include <sstream>

#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>


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

    std::stringstream s;        // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    s.exceptions(std::ios::failbit);

    {
        std::string src = "1";

        std::string dst;

        ReadBuffer in(src.data(), src.size(), 0);

        LimitReadBuffer limit_in(in, 1, false);

        {
            WriteBufferFromString out(dst);

            copyData(limit_in, out);
        }

        if (limit_in.count() != 1)
        {
            s << "Failed!, incorrect count(): " << limit_in.count();
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }

        if (in.count() != limit_in.count())
        {
            s << "Failed!, incorrect underlying buffer's count(): " << in.count();
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }
        if (src != dst)
        {
            s << "Failed!, incorrect destination value, read: " << dst << ", expected: " << src;
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }
    }
    {
        std::string src = "abc";
        ReadBuffer in(src.data(), src.size(), 0);

        std::string dst;

        {
            WriteBufferFromString out(dst);

            char x;
            readChar(x, in);

            LimitReadBuffer limit_in(in, 1, false);

            copyData(limit_in, out);


            if (in.count() != 2)
            {
                s << "Failed!, Incorrect underlying buffer's count: " << in.count() << ", expected: " << 2;
                throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
            }

            if (limit_in.count() != 1)
            {
                s << "Failed!, Incorrect count: " << limit_in.count() << ", expected: " << 1;
                throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (dst != "b")
        {
            s << "Failed!, Incorrect destination value: " << dst << ", expected 'b'";
            throw Exception(dst, ErrorCodes::LOGICAL_ERROR);
        }

        char y;
        readChar(y, in);
        if (y != 'c')
        {
            s << "Failed!, Read incorrect value from underlying buffer: " << y << ", expected 'c'";
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }
        while (!in.eof())
            in.ignore();
        if (in.count() != 3)
        {
            s << "Failed!, Incorrect final count from underlying buffer: " << in.count() << ", expected: 3";
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    {
        std::string src = "abc";
        ReadBuffer in(src.data(), src.size(), 0);

        {
            LimitReadBuffer limit_in(in, 1, false);

            char x;
            readChar(x, limit_in);

            if (limit_in.count() != 1)
            {
                s << "Failed!, Incorrect count: " << limit_in.count() << ", expected: " << 1;
                throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (in.count() != 1)
        {
            s << "Failed!, Incorrect final count from underlying buffer: " << in.count() << ", expected: 1";
            throw Exception(s.str(), ErrorCodes::LOGICAL_ERROR);
        }
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
