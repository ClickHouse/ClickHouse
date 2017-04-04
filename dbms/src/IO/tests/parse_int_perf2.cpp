#include <iostream>
#include <iomanip>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>

#include <Common/Stopwatch.h>


namespace test
{
    template <typename T>
    void readIntText(T & x, DB::ReadBuffer & buf)
    {
        bool negative = false;
        x = 0;

        if (unlikely(buf.eof()))
            DB::throwReadAfterEOF();

        if (std::is_signed<T>::value && *buf.position() == '-')
        {
            ++buf.position();
            negative = true;
        }

        if (*buf.position() == '0')
        {
            ++buf.position();
            return;
        }

        while (!buf.eof())
        {
            if ((*buf.position() & 0xF0) == 0x30)
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                break;
        }

        if (std::is_signed<T>::value && negative)
            x = -x;
    }
}


int main(int argc, char ** argv)
{
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
        Int64 n = 0;
        size_t nums = 0;

        Stopwatch watch;

        while (!in.eof())
        {
            DB::readIntText(n, in);
            in.ignore();

            //std::cerr << "n: " << n << std::endl;

            ++nums;
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Read " << nums << " numbers (" << in.count() / 1000000.0 << " MB) in " << watch.elapsedSeconds() << " sec., "
            << nums / watch.elapsedSeconds() << " num/sec. (" << in.count() / watch.elapsedSeconds() / 1000000 << " MB/s.)"
            << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
