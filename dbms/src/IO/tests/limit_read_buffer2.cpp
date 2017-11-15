#include <sstream>

#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>


int main(int argc, char ** argv)
{
    try
    {
        std::stringstream s;

        {
            std::string src = "1";

            std::string dst;

            DB::ReadBuffer in(&src[0], src.size(), 0);

            DB::LimitReadBuffer limit_in(in, 1);

            {
                DB::WriteBufferFromString out(dst);

                DB::copyData(limit_in, out);
            }

            if (limit_in.count() != 1)
            {
                s << "Failed!, incorrect count(): " << limit_in.count();
                throw DB::Exception(s.str());
            }

            if (in.count() != limit_in.count())
            {
                s << "Failed!, incorrect underlying buffer's count(): " << in.count();
                throw DB::Exception(s.str());
            }
            if (src != dst)
            {
                s << "Failed!, incorrect destination value, read: " << dst << ", expected: " << src;
                throw DB::Exception(s.str());
            }
        }
        {
            std::string src = "abc";
            DB::ReadBuffer in(&src[0], src.size(), 0);

            std::string dst;

            {
                DB::WriteBufferFromString out(dst);

                char x;
                DB::readChar(x, in);

                DB::LimitReadBuffer limit_in(in, 1);

                DB::copyData(limit_in, out);


                if (in.count() != 2)
                {
                    s << "Failed!, Incorrect underlying buffer's count: " << in.count() << ", expected: " << 2;
                    throw DB::Exception(s.str());
                }

                if (limit_in.count() != 1)
                {
                    s << "Failed!, Incorrect count: " << limit_in.count() << ", expected: " << 1;
                    throw DB::Exception(s.str());
                }
            }

            if (dst != "b")
            {
                s << "Failed!, Incorrect destination value: " << dst << ", expected 'b'";
                throw DB::Exception(dst);
            }

            char y;
            DB::readChar(y, in);
            if (y != 'c')
            {
                s << "Failed!, Read incorrect value from underlying buffer: " << y << ", expected 'c'";
                throw DB::Exception(s.str());
            }
            while (!in.eof())
                in.ignore();
            if (in.count() != 3)
            {
                s << "Failed!, Incorrect final count from underlying buffer: " << in.count() << ", expected: 3";
                throw DB::Exception(s.str());
            }
        }

        {
            std::string src = "abc";
            DB::ReadBuffer in(&src[0], src.size(), 0);

            {
                DB::LimitReadBuffer limit_in(in, 1);

                char x;
                DB::readChar(x, limit_in);

                if (limit_in.count() != 1)
                {
                    s << "Failed!, Incorrect count: " << limit_in.count() << ", expected: " << 1;
                    throw DB::Exception(s.str());
                }
            }

            if (in.count() != 1)
            {
                s << "Failed!, Incorrect final count from underlying buffer: " << in.count() << ", expected: 1";
                throw DB::Exception(s.str());
            }
        }

    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
