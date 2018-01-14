#include <string>

#include <iostream>
#include <fstream>

#include <Core/Types.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <IO/readFloatText.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/CompressedReadBuffer.h>


/** How to test:

# Prepare data

$ clickhouse-local --query="SELECT number FROM system.numbers LIMIT 10000000" > numbers1.tsv
$ clickhouse-local --query="SELECT number % 10 FROM system.numbers LIMIT 10000000" > numbers2.tsv
$ clickhouse-local --query="SELECT number / 1000 FROM system.numbers LIMIT 10000000" > numbers3.tsv
$ clickhouse-local --query="SELECT rand64() / 1000 FROM system.numbers LIMIT 10000000" > numbers4.tsv
$ clickhouse-local --query="SELECT rand64() / 0xFFFFFFFF FROM system.numbers LIMIT 10000000" > numbers5.tsv
$ clickhouse-local --query="SELECT log(rand64()) FROM system.numbers LIMIT 10000000" > numbers6.tsv
$ clickhouse-local --query="SELECT 1 / rand64() FROM system.numbers LIMIT 10000000" > numbers7.tsv
$ clickhouse-local --query="SELECT exp(rand() / 0xFFFFFF) FROM system.numbers LIMIT 10000000" > numbers8.tsv
$ clickhouse-local --query="SELECT number FROM system.numbers LIMIT 10000000" > numbers1.tsv
$ clickhouse-local --query="SELECT toString(rand64(1)) || toString(rand64(2)) || toString(rand64(3)) || toString(rand64(4)) FROM system.numbers LIMIT 10000000" > numbers9.tsv
$ clickhouse-local --query="SELECT toString(rand64(1)) || toString(rand64(2)) || '.' || toString(rand64(3)) || toString(rand64(4)) FROM system.numbers LIMIT 10000000" > numbers10.tsv

# Run test

$ for i in {1..10}; do echo $i; time ./read_float_perf < numbers$i.tsv; done

  */

using namespace DB;


template <typename T, typename ReturnType>
ReturnType readFloatTextFastImpl2(T & x, ReadBuffer & in)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    UInt64 digits = 0;

    constexpr int max_significant_digits = std::numeric_limits<UInt64>::digits10;

    size_t num_significant_digits = 0;
    size_t num_excessive_digits = 0;
    ssize_t position_of_point = -1;
    bool in_leading_zeros = true;
    bool in_exponent = false;
    bool exponent_is_negative = false;
    int exponent = 0;
    size_t position_after_digits = 0;

    while (!in.eof())
    {
        if ((*in.position() & 0xF0) == 0x30)
        {
            if (unlikely(in_exponent))
            {
                auto digit = *in.position() & 0x0F;

                exponent *= 10;
                exponent += digit;
            }
            else if (num_significant_digits < max_significant_digits)
            {
                auto digit = *in.position() & 0x0F;

                digits *= 10;
                digits += digit;

                if (digit)
                    in_leading_zeros = false;
                if (!in_leading_zeros)
                    ++num_significant_digits;
            }
            else
                ++num_excessive_digits;
        }
        else if (*in.position() == '-')
        {
            if (unlikely(in_exponent))
                exponent_is_negative = true;
            else
                negative = true;
        }
        else if (*in.position() == '.')
        {
            position_of_point = in.count();
        }
        else if (*in.position() == 'e' || *in.position() == 'E')
        {
            in_exponent = true;
            if (!position_after_digits)
                position_after_digits = in.count();
        }
        else if (*in.position() == 'i' || *in.position() == 'I')
        {
            if (assertOrParseInfinity<throw_exception>(in))
            {
                x = std::numeric_limits<T>::infinity();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
        else if (*in.position() == 'n' || *in.position() == 'N')
        {
            if (assertOrParseNaN<throw_exception>(in))
            {
                x = std::numeric_limits<T>::quiet_NaN();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
        else
        {
            if (!position_after_digits)
                position_after_digits = in.count();
            break;
        }

        ++in.position();
    }

    if (exponent_is_negative)
        exponent = -exponent;

    exponent += num_excessive_digits;

    if (position_of_point != -1)
        exponent -= position_after_digits - position_of_point - 1;

    x = shift10(digits, exponent);

    if (negative)
        x = -x;

    return ReturnType(true);
}


template <typename T, void F(T&, ReadBuffer&)>
void NO_INLINE loop(ReadBuffer & in, WriteBuffer & out)
{
    T sum = 0;
    T x = 0;

    Stopwatch watch;

    while (!in.eof())
    {
        F(x, in);
        in.ignore();
        sum += x;
    }

    watch.stop();
    out << "Read in " << watch.elapsedSeconds() << " sec, " << formatReadableSizeWithBinarySuffix(in.count() / watch.elapsedSeconds()) << "/sec, result = " << sum << "\n";
}


int main(int argc, char ** argv)
try
{
    int method = 0;
    if (argc >= 2)
        method = parse<int>(argv[1]);

    using T = Float64;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    if (method == 1) loop<T, readFloatTextPrecise>(in, out);
    if (method == 2) loop<T, readFloatTextFast>(in, out);
    if (method == 3) loop<T, readFloatTextSimple>(in, out);
    if (method == 4) loop<T, readFloatTextFastImpl2<Float64, void>>(in, out);

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
