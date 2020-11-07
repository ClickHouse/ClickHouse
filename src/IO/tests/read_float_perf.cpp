#include <string>

#include <iostream>
#include <fstream>

#include <common/types.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <IO/readFloatText.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Compression/CompressedReadBuffer.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

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

$ for i in {1..10}; do echo $i; time ./read_float_perf 2 < numbers$i.tsv; done

  */


using namespace DB;

#ifdef USE_FAST_FLOAT
#include <fast_float/fast_float.h>

template <typename T, typename ReturnType>
ReturnType readFloatTextFastFloatImpl(T & x, ReadBuffer & in)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII"); //-V590

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    String buff;
    
    /// TODO: Optimize 
    /// Currently fast_float interface need begin and end
    /// ReadBuffers current begin end can have only part of data
    while (!in.eof() && isAlphaNumericASCII(*in.position()))  {
        buff += *in.position();
        ++in.position();
    }

    if (buff.empty())
    {
        
        if constexpr (throw_exception)
            throw Exception("Cannot read floating point value: no digits read", ErrorCodes::CANNOT_PARSE_NUMBER);
        else
            return ReturnType(false);
  
    }

    auto res = fast_float::from_chars(buff.data(), buff.data() + buff.size(), x);

    if (res.ec != std::errc())
    {
        if constexpr (throw_exception)
            throw Exception("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
        else
            return ReturnType(false);
    }

    return ReturnType(true);
}

#endif


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
    out << "Read in " << watch.elapsedSeconds() << " sec, "
        << formatReadableSizeWithBinarySuffix(in.count() / watch.elapsedSeconds()) << "/sec, result = " << sum << "\n";
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
    #ifdef USE_FAST_FLOAT
    if (method == 4) loop<T, readFloatTextFastFloatImpl>(in, out);
    #endif

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
